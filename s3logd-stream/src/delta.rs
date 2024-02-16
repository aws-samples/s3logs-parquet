use std::collections::HashMap;
use tokio::io::Error;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::file::properties::WriterProperties;
use deltalake::writer::record_batch::RecordBatchWriter;
use deltalake::writer::DeltaWriter;
use deltalake::table::DeltaTable;
use crate::output::AsyncOutput;

type Result<T> = std::result::Result<T, Error>;

pub(crate) struct AsyncDeltaOutput {
    inner: RecordBatchWriter,
    table: DeltaTable
}

impl AsyncDeltaOutput {
    pub async fn new(date_partition_prefix: &str, schema_ref: SchemaRef, writer_props: WriterProperties) -> Self {

        let mut storage_opt = HashMap::new();
        let region = std::env::var("DELTALAKE_AWS_REGION").expect("env DELTALAKE_AWS_REGION not found");
        storage_opt.insert("AWS_REGION".to_string(), region);

        let ak = std::env::var("DELTALAKE_AWS_ACCESS_KEY_ID").expect("env DELTALAKE_AWS_ACCESS_KEY_ID not found");
        storage_opt.insert("AWS_ACCESS_KEY_ID".to_string(), ak);

        let sk = std::env::var("DELTALAKE_AWS_SECRET_ACCESS_KEY").expect("env DELTALAKE_AWS_SECRET_ACCESS_KEY not found");
        storage_opt.insert("AWS_SECRET_ACCESS_KEY".to_string(), sk);

        let lock = std::env::var("DELTALAKE_AWS_S3_LOCKING_PROVIDER").expect("env DELTALAKE_AWS_S3_LOCKING_PROVIDER not found");
        if lock != "dynamodb" {
            panic!("invalid AWS_S3_LOCKING_PROVIDER {}, only accept 'dynamodb'", lock);
        }
        storage_opt.insert("AWS_S3_LOCKING_PROVIDER".to_string(), lock);

        let tbl = std::env::var("DELTALAKE_DYNAMO_LOCK_TABLE_NAME").expect("env DELTALAKE_DYNAMO_LOCK_TABLE_NAME not found");
        storage_opt.insert("DYNAMO_LOCK_TABLE_NAME".to_string(), tbl);

        let tbl_uri = std::env::var("DELTALAKE_TABLE_S3_LOCATION").expect("env DELTALAKE_TABLE_S3_LOCATION not found");

        let maybe_table = deltalake::open_table_with_storage_options(&tbl_uri, storage_opt.clone()).await;
        let delta_table = match maybe_table {
            Ok(table) => table,
            Err(err) => panic!("{:?}", err),
        };

        // get back partition column name
        let s: Vec<&str> = date_partition_prefix.split('=').collect();
        let pc = vec![s[0].to_string()];

        let writer = RecordBatchWriter::try_new(
                        tbl_uri, // table_uri
                        schema_ref, // schema
                        Some(pc), // partition_columns
                        Some(storage_opt), // storage_options
                    )
                    .expect("unable to create RecordBatchWriter")
                    .with_writer_properties(writer_props);

        Self {
            inner: writer,
            table: delta_table,
        }
    }
}

impl AsyncOutput for AsyncDeltaOutput {
    async fn write(&mut self, batch: RecordBatch, partition_str: &str) -> Result<()> {
        let mut part = HashMap::new();
        let s: Vec<&str> = partition_str.split('=').collect();
        part.insert(s[0].to_string(), Some(s[1].to_string()));
        match self.inner.write_partition(batch, &part).await {
            Ok(()) => {
                return Ok(());
            },
            Err(e) => {
                panic!("write partition error: {:?}", e);
            }
        }
    }

    async fn close(mut self) -> Result<()> {
        match self.inner.flush_and_commit(&mut self.table).await {
            Ok(_) => {
                return Ok(());
            },
            Err(e) => {
                panic!("close writer error: {:?}", e);
            }
        }
    }
}
