/*
const S3LOGS_CONFIG_DEFAULT_PARQUET_SCHEMA_FILE: &str = "parquet.schema";
const S3LOGS_CONFIG_DEFAULT_PARQUET_WRITER_PROPERTIES_FILE: &str = "parquet_writer_properties.ini";


let config_root = env::var("S3LOGS_CONFIG_ROOT_PATH")
                            .unwrap_or(S3LOGS_CONFIG_DEFAULT_ROOT_PATH.to_string());
let parquet_schema_file = env::var("S3LOGS_CONFIG_PARQUET_SCHEMA_FILE")
                    .unwrap_or(S3LOGS_CONFIG_DEFAULT_PARQUET_SCHEMA_FILE.to_string());
let schema_filepath = format!("{}/{}", config_root, parquet_schema_file);

        let message_type = std::fs::read_to_string(&schema_filepath).expect("unable to read parquet schema config");
        let pq_schema = parquet::schema::parser::parse_message_type(&message_type).expect("Expected valid schema");

        // convert parquet schemd to arrow schema
        let schema_desc = parquet::schema::types::SchemaDescriptor::new(Arc::new(pq_schema));
        let schema = parquet::arrow::parquet_to_arrow_schema(&schema_desc, None).expect("unable to convert schema from parquet to arrow");

        // define writer props
        let parquet_writer_props_file = env::var("S3LOGS_CONFIG_PARQUET_WRITER_PROPERTIES_FILE")
                            .unwrap_or(S3LOGS_CONFIG_DEFAULT_PARQUET_WRITER_PROPERTIES_FILE.to_string());
        let wr_props_fullpath = format!("{}/{}", config_root, parquet_writer_props_file);
        let writer_props = ParquetWriterConfigReader::new(&wr_props_fullpath);
*/

/*
pub async fn output_task(ctx: OutputContext, config: WriterConfig) -> Result<()> {

    ctx.complete.output_task_start();

    let buffer_size = 100 * 1024 * 1024;
    let _output_file = output_file.to_string();
    let f = std::fs::File::create(output_file).unwrap();
    let writer = std::io::BufWriter::with_capacity(buffer_size, f);
    let mut parquet = utils::NewParquetOutput::new(writer);

    while ctx.quit.load(Ordering::SeqCst) != true {

        // TODO check writer rotate condition
        // 1. lines
        // 2. time
        //
        if parquet.need_rotation() {
            parquet = parquet.rotate();
        }

        match ctx.output_channel.try_recv() {
            Ok(lines) => {
                parquet.write(lines).await;
                continue;
            },
            Err(crossbeam::channel::TryRecvError::Empty) => {
                continue;
            },
            Err(crossbeam::channel::TryRecvError::Disconnected) => {
                warn!("consumer channel disconnected in output task");
                break;
            }
        }
    }

    info!("closing parquet file before quit, pls be patient");
    parquet.close().unwrap();
    info!("output parquet file {} closed", _output_file);
    ctx.complete.output_task_complete();
    Ok(())
}
*/

pub struct AsyncParquetOutput<W> {
    writer: AsyncArrowWriter<W>,
    ctx: OutputContext,
}

impl<W: AsyncWrite + Unpin + Send> AsyncParquetOutput<W> {

    pub fn new(buf_wr: W, buffer_size: usize, schema: Schema, writer_props: WriterProperties, ctx: OutputContext) -> Self {

        // build writer
        let writer = AsyncArrowWriter::try_new(buf_wr, Arc::clone(&schema_ref), buffer_size, Some(writer_props.clone())).unwrap();

        Self {
            schema_ref: schema_ref,
            writer_props: writer_props,
            writer: writer,
            ctx: ctx,
        }

    }

    pub async fn simple_output_loop(&mut self) -> Result<()> {

        while self.ctx.quit.load(Ordering::SeqCst) != true {

            match self.ctx.output_channel.try_recv() {
                Ok(lines) => {
                    self.append_lines(lines).await;
                    continue;
                },
                Err(crossbeam::channel::TryRecvError::Empty) => {
                    continue;
                },
                Err(crossbeam::channel::TryRecvError::Disconnected) => {
                    warn!("consumer channel disconnected in output task");
                    break;
                }
            }
            self.writer.flush().await;
        }

        info!("closing parquet file before quit, pls be patient");
        self.writer.close().await;
        //info!("output parquet file {} closed", _output_file);
        //ctx.complete.output_task_complete();
        Ok(())
    }

    pub fn lines_to_v(&self, lines: Vec<Lines>) -> Vec<Vec<String>> {
        let v = lines.filter_map(|l| {
            let line = self.line_parser.extract_full(&l.unwrap(), false);
            Some(line)
        }).collect::<vec<Vec<String>>>();
        v
    }

    pub fn vec_to_columns(&self, v: Vec<Vec<String>>, max: usize) -> ArrowResult<Vec<ArrayRef>> {
        let null = "".to_string();
        let arrays: ArrowResult<Vec<ArrayRef>> = (0..max)
                                                .map(|idx| {
                                                    Ok(Arc::new(v.iter()
                                                            .map(|row| row.get(idx).or(Some(&null)))
                                                            .collect::<StringArray>(),) as ArrayRef)
                                                })
                                                .collect();
        arrays
    }

    pub async fn append_lines(&mut self, v: Vec<Vec<String>>, max: usize) -> Result<()> {

        let columns = self.vec_to_columns(v);
        let batch = RecordBatch::try_new(Arc::clone(&self.schema_ref), columns).unwrap();
        let res = self.writer.write(&batch).await;
        if res.is_ok() {
            return Ok(());
        } else {
            warn!("parquet writer write op failed {:?}", res.unwrap());
        }

        Ok(())
    }

    pub async fn close(self) -> Result<()> {
        let _ = self.writer.close().await;

        Ok(())
    }
}
