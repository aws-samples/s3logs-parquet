use tokio::io::Error;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use parquet::file::properties::WriterProperties;
use parquet::arrow::async_writer::AsyncArrowWriter;
use crate::output::AsyncOutput;

type Result<T> = std::result::Result<T, Error>;

pub(crate) struct AsyncArrowOutput {
    inner: AsyncArrowWriter<tokio::fs::File>,
}

impl AsyncArrowOutput {
    pub fn new(buf_wr: tokio::fs::File, buffer_size: usize,
            schema_ref: SchemaRef, writer_props: WriterProperties) -> Self {

        let writer = AsyncArrowWriter::try_new(buf_wr, schema_ref, buffer_size, Some(writer_props)).unwrap();
        Self {
            inner: writer,
        }
    }
}

impl AsyncOutput for AsyncArrowOutput {
    async fn write(&mut self, batch: RecordBatch, _: &str) -> Result<()> {
        match self.inner.write(&batch).await {
            Ok(()) => {
                return Ok(());
            },
            Err(e) => {
                panic!("write partition error: {:?}", e);
            }
        }
    }

    async fn close(self) -> Result<()> {
        match self.inner.close().await {
            Ok(_) => {
                return Ok(());
            },
            Err(e) => {
                panic!("close writer error: {:?}", e);
            }
        }
    }
}
