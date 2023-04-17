use std::time::Instant;
use lambda_runtime::{run, service_fn, Error, LambdaEvent};
use tracing_subscriber::EnvFilter;
use serde::{Deserialize, Serialize};
use s3logs::utils::S3LogTransform;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct EventBridgeEvent {
    pub region: String,
    pub bucket: String,
}

async fn function_handler(event: LambdaEvent<EventBridgeEvent>) -> Result<(), Error> {

    let eb_event = event.payload;

    let region = eb_event.region;
    let bucket = eb_event.bucket;

    println!("start log transform task for region: {} to bucket: {}", region, bucket);
    let now = Instant::now();
    let trans = S3LogTransform::new(&region, &bucket, None, None, None);
    match trans.process_stagging_dir().await {
        Ok(total_lines) => {
            println!("transform task ended, {} lines processed cost: {:?}", total_lines, now.elapsed());
        },
        Err(err) => {
            println!("transform task failed with err: {:?}", err);
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    run(service_fn(function_handler)).await
}
