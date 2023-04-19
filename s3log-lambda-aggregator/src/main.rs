use std::sync::{Arc, Mutex};
use futures::future::join_all;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use aws_lambda_events::event::sqs::SqsEvent;
use aws_lambda_events::s3::{S3Event, S3EventRecord};
use tracing_subscriber::EnvFilter;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use s3logs::utils::S3LogAggregator;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct BatchItemFailures {
    #[serde(rename = "batchItemFailures")]
    pub batch_item_failures: Vec<ItemIdentifier>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct ItemIdentifier {
    #[serde(rename = "itemIdentifier")]
    pub item_identifier: String,
}

#[derive(Debug, Serialize)]
struct FailureMsg {
    pub body: String,
}

impl std::fmt::Display for FailureMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.body)
    }
}

impl std::error::Error for FailureMsg {}

async fn s3_event_handler(record: S3EventRecord) -> Result<(), Error> {

    let region = record.aws_region.unwrap();
    let bucket = record.s3.bucket.name.unwrap();
    let key = record.s3.object.key.unwrap();
    let size = record.s3.object.size.unwrap();
    let event_name = record.event_name.unwrap();

    if event_name != "ObjectCreated:CompleteMultipartUpload"
        && event_name != "ObjectCreated:Put" {

        println!("skip non PUT event: {} for region: {}, bucket: {}, key: {}, size: {}",
                    event_name, region, bucket, key, size);
        // skip non new log event
        return Ok(())
    }

    println!("start log aggregation task for region: {}, bucket: {}, key: {}, size: {}",
                    region, bucket, key, size);
    let agg = S3LogAggregator::new(&region, &bucket, &key, None, None, None, None);
    let res = agg.process_s3().await;

    if res.is_err() {
        return Err(Box::new(FailureMsg {
                            body: format!("{:?}", res.unwrap_err()),
                        }));
    }
    println!("{} {} of lines processed", key, res.unwrap());

    Ok(())
}

async fn sqs_event_handler(event: SqsEvent) -> Result<Value, Error> {

    let failed_message: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let mut tasks = Vec::with_capacity(event.records.len());

    for record in event.records {
        if let Ok(s3events) = serde_json::from_str::<S3Event>(record.body.as_ref().unwrap()) {
            for s3rec in s3events.records {
                let failed_message = failed_message.clone();
                let message_id = record.message_id.as_ref().unwrap().clone();
                failed_message.lock().unwrap().push(message_id.clone());
                tasks.push(tokio::spawn(async move {
                    if let Ok(_) = s3_event_handler(s3rec).await {
                        // remove message in failed message vec
                        failed_message
                            .lock()
                            .unwrap()
                            .retain(|x| *x != message_id);
                    }
                }));
            }
        }
    }

    join_all(tasks).await;

    let resp = BatchItemFailures {
        batch_item_failures: failed_message
            .lock()
            .unwrap()
            .clone()
            .into_iter()
            .map(|message_id| {
                return ItemIdentifier {
                    item_identifier: message_id,
                };
            })
            .collect(),
    };

    Ok(serde_json::to_value(resp).unwrap())
}

async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<Value, Error> {

    let value = sqs_event_handler(event.payload).await?;

    let failed = value["batchItemFailures"].as_array().unwrap();

    if failed.len() > 0 {
        println!("lambda function handler return batch faliures: {:?}", failed);
    }

    Ok(value)
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(true)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    run(service_fn(function_handler)).await?;

    Ok(())
}
