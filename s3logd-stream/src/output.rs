use std::io::BufRead;
use std::collections::{HashMap, BTreeMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::io::AsyncWrite;
use tokio::io::{Error, ErrorKind};
use tokio::io::{BufReader, AsyncReadExt, AsyncWriteExt, AsyncBufReadExt};
use log::{info, warn, debug, error};
use arrow::error::Result as ArrowResult;
use arrow::array::{ArrayRef, StringArray};
use arrow::record_batch::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use parquet::arrow::async_writer::AsyncArrowWriter;
use parquet::file::properties::WriterProperties;
use chrono::prelude::*;
use crossbeam::channel::{Sender, Receiver};
use serde::{Deserialize, Serialize};
use rand::distributions::{Alphanumeric, DistString};
use aws_sdk_sqs::{Client, Region};
use s3logs::utils::LineParser;
use s3logs::stats::TimeStats;
use s3logs::transfer::TransferManager;
use crate::conf::ParquetWriterConfigReader;

const S3_LOG_DATATIME_FMT: &str = "%d/%b/%Y:%H:%M:%S %z";

const file_receipt_dir: &str = "/backup/stream";
const output_temp_dir:&str = "/backup/stream";
const hourly_partition: bool = false;
const timezone: &str = "UTC+8";
const output_threshold_lines: usize = 10000000;
const output_threshold_maxidle: u64 = 60;

pub type Result<T> = std::result::Result<T, Error>;
type LogFields = Vec<String>;
type TimeStamp = usize;
type PartitionedTimeStamp = usize;

#[derive(Clone)]
struct TimePartition {
    tz: FixedOffset,
    align_mask: TimeStamp,
    parse_func: fn(&Self, &str) -> PartitionedTimeStamp,
}

impl TimePartition {
    fn new(tz_str: &str, hourly: bool) -> Self {
        let ptz = tzif::parse_posix_tz_string(tz_str.as_bytes());
        let utc_offset = ptz.unwrap().std_info.offset.0;
        let tz = FixedOffset::east_opt(utc_offset as i32).unwrap();
        let align_mask = if hourly {
            // hourly
            3600
        } else {
            // daily
            86400
        };

        let parse_func = if utc_offset == 0 {
            Self::parser_utc0
        } else {
            Self::parser_custom
        };

        Self {
            tz: tz,
            align_mask: align_mask,
            parse_func: parse_func,
        }
    }

    #[inline]
    fn timestamp_align_left(&self, ts: TimeStamp) -> PartitionedTimeStamp {
        ts - ts % self.align_mask
    }

    fn parser_custom(&self, reqtime: &str) -> PartitionedTimeStamp {
        let dt = DateTime::parse_from_str(reqtime, S3_LOG_DATATIME_FMT).unwrap().with_timezone(&self.tz);
        let local: NaiveDateTime = dt.naive_local();
        let ts = local.timestamp();
        self.timestamp_align_left(ts as TimeStamp)
    }

    fn parser_utc0(&self, reqtime: &str) -> PartitionedTimeStamp {
        let ts = DateTime::parse_from_str(reqtime, S3_LOG_DATATIME_FMT).unwrap().timestamp();
        self.timestamp_align_left(ts as TimeStamp)
    }

    // parse function wrapper
    fn parse(&self, reqtime: &str) -> PartitionedTimeStamp {
        (self.parse_func)(&self, reqtime)
    }
}

#[derive(Serialize, Deserialize)]
#[derive(Debug, Clone, PartialEq)]
enum ReceiptType {
    Sqs,
    SqsHybrid,
    File,
}

#[derive(Serialize, Deserialize)]
#[derive(Clone)]
pub struct Receipt {
    type_: ReceiptType,
    region: String,
    bucket: String,
    key: String,
    sqs_url: String,
    line_start: usize,
    line_count: usize,
    sqs_receipt: String,
    file_receipt: String,
    #[serde(skip)]
    client: Option<Arc<Client>>,
}

impl Receipt {

    async fn finalize(&mut self, start: usize, count: usize) {
        self.line_start = start;
        self.line_count = count;

        if self.type_ == ReceiptType::Sqs {
            return;
        }

        // gen a file receipt
        let file_path = format!("{}/file_{}.receipt", file_receipt_dir, Alphanumeric.sample_string(&mut rand::thread_rng(), 16));

        self.file_receipt = file_path;

        let data = serde_json::to_string(&self).expect("failed to serialize receipt");

        let res = tokio::fs::File::create(&self.file_receipt).await;
        if res.is_err() {
            panic!("unable to create file receipt reason: {:?}", res.unwrap());
        }

        let mut file = res.unwrap();
        file.write(&data.into_bytes()).await;
    }

    async fn del_sqs_receipt(&self) {

        let res = self.client.clone().unwrap()
                        .delete_message()
                        .queue_url(&self.sqs_url)
                        .receipt_handle(&self.sqs_receipt)
                        .send()
                        .await;
        if res.is_err() {
            match &res {
                Err(aws_sdk_sqs::types::SdkError::ServiceError { err, ..}) => match err.kind {
                    _ => warn!("sdk error: {}", err)
                },
                Err(e) => {
                    warn!("error: {}", e)
                },
                _ => panic!(),
            }
            panic!("failed to delete sqs receipt");
        }
    }

    async fn del_file_receipt(&self) {

        let res = tokio::fs::remove_file(&self.file_receipt).await;
        if res.is_err() {
            panic!("failed to delete file receipt {}", self.file_receipt);
        }
    }

    async fn close(&self) {
        match self.type_ {
            ReceiptType::Sqs => {
                self.del_sqs_receipt().await;
            },
            ReceiptType::SqsHybrid => {
                self.del_sqs_receipt().await;
                self.del_file_receipt().await;
            },
            ReceiptType::File => {
                self.del_file_receipt().await;
            },
        }
    }

    fn set_client(&mut self, client: Arc<Client>) {
        self.client = Some(client)
    }
}

struct ReceiptGen {
    default: Receipt,
    counter: usize,
    client: Arc<Client>,
}

impl ReceiptGen {
    fn new(sqs_url: &str, client: Arc<Client>, region: &str, bucket: &str, key: &str, sqs_receipt: &str) -> Self {
        Self {
            default: Receipt {
                type_: ReceiptType::Sqs,
                region: region.to_string(),
                bucket: bucket.to_string(),
                key: key.to_string(),
                sqs_url: sqs_url.to_string(),
                line_start: 0,
                line_count: 0,
                sqs_receipt: sqs_receipt.to_string(),
                file_receipt: String::new(),
                client: None,
            },
            counter: 0,
            client: client.clone(),
        }
    }

    async fn next(&mut self, total: usize) -> Receipt {

        assert!(total > 0);

        let mut receipt = self.default.clone();

        if total == 1 {
            // ReceiptType::Sqs
            receipt.set_client(self.client.clone());
            return receipt;
        }

        if self.counter == 0 {
            receipt.type_ = ReceiptType::SqsHybrid;
            receipt.set_client(self.client.clone());
        } else {
            receipt.type_ = ReceiptType::File;
        }

        self.counter += 1;
        return receipt;
    }

}

#[derive(PartialEq)]
enum ChannelGateState {
    Initialized,
    Open,
    Closing,
    Closed,
}

#[derive(Clone)]
struct Channel {
    //state: Arc<Mutex<ChannelGateState>>,
    tx_count: Arc<AtomicUsize>,
    rx_count: Arc<AtomicUsize>,
    tx: Sender<Vec<LogFields>>,
    rx: Receiver<Vec<LogFields>>,
    receipts: Arc<Mutex<Vec<Receipt>>>,
}

impl Channel {
    pub fn new() -> Self {
        let (tx, rx) = crossbeam::channel::unbounded();
        Self {
            //state: Arc::new(Mutex::new(ChannelGateState::Initialized)),
            tx_count: Arc::new(AtomicUsize::new(0)),
            rx_count: Arc::new(AtomicUsize::new(0)),
            tx: tx,
            rx: rx,
            receipts: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn inc_sender(&self) {
        self.tx_count.fetch_add(1, Ordering::SeqCst);
    }

    fn dec_sender(&self) {
        self.tx_count.fetch_sub(1, Ordering::SeqCst);
    }

    fn count_sender(&self) -> usize {
        self.tx_count.load(Ordering::SeqCst)
    }

    fn count_queue(&self) -> usize {
        self.rx.len()
    }

    fn inc_rx(&self) {
        self.rx_count.fetch_add(1, Ordering::SeqCst);
    }

    fn dec_rx(&self) {
        self.rx_count.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn get_sender(&self) -> Sender<Vec<LogFields>> {
        self.inc_sender();
        self.tx.clone()
    }

    pub fn put_sender(&self) {
        self.dec_sender();
    }

    pub fn get_rx(&self) -> Receiver<Vec<LogFields>> {
        self.inc_rx();
        self.rx.clone()
    }

    pub async fn do_send(&self, logs: Vec<LogFields>, receipt: Receipt) {
        let res = self.get_sender().try_send(logs);
        if res.is_err() {
            warn!("err while send logs to output: {:?}", res.unwrap());
            todo!();
        }

        let mut receipts = self.receipts.lock().await;
        receipts.push(receipt);

        self.put_sender();
    }

    pub async fn close(mut self) {
        
        let mut receipts = self.receipts.lock().await;

        while let Some(receipt) = receipts.pop() {
            receipt.close().await;
        }
    }
}

#[derive(Clone)]
struct Channels {
    inner: Arc<RwLock<HashMap<usize, Channel>>>,
}

impl Channels {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn get(&self, ts: TimeStamp) -> Option<Channel> {
        let inner = self.inner.read().await;
        inner.get(&ts).clone().cloned()
    }

    /*
    pub async fn send_logs(&self, ts: TimeStamp, logs: Vec<LogFields>) -> Result<()> {

        // select date partition
        if let Some(tx) = self.get(ts).await {
            let res = tx.send(logs);
            if res.is_err() {
                warn!("err while send logs to output: {:?}", res.unwrap());
                todo!();
            }
            return Ok(());
        }
        return Err(Error::new(ErrorKind::Other, "failed to send logs to output"));
    }
    */

    async fn create(&self, ts: TimeStamp) -> Receiver<Vec<LogFields>> {
        let mut inner = self.inner.write().await;
        if let Some(channel) = inner.get(&ts) {
            // channel already exists
            return channel.get_rx()
        }

        let channel = Channel::new();
        let rx = channel.get_rx();
        inner.insert(ts, channel);
        rx
    }

    // remove channel and return state of this channel
    // return: None for clean, Some for not clean
    async fn remove(&self, ts: TimeStamp) -> Channel {
        let mut inner = self.inner.write().await;
        if let Some(channel) = inner.remove(&ts) {
            debug!("[{}] channel removed", ts);
            return channel;
        }
        panic!("[{}] not able to remove channel", ts);
    }
}

#[derive(Clone)]
struct WriterConfig {
    schema_ref: SchemaRef,
    writer_props: WriterProperties,
}

pub struct Context {
    quit: Arc<AtomicBool>,
}

#[derive(Clone)]
pub struct Manager {
    bucket: String,
    region: String,
    prefix: String,
    line_parser: LineParser,
    time_partition: TimePartition,
    config: WriterConfig,
    chans: Channels,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
    quit: Arc<AtomicBool>,
    permit: Arc<tokio::sync::Semaphore>,
    tm: TransferManager,
}

impl Manager {

    pub fn new(quit: Arc<AtomicBool>) -> Self {

        let wr_props_fullpath = "/tmp/parquet_writer_properties.ini";
        let schema_filepath = "/tmp/parquet.schema";
        let bucket = "ahaparquet";
        let region = "ap-northeast-1";
        let prefix = "s3logs";
        let sqs_url = "https://sqs.ap-northeast-1.amazonaws.com/911329921905/s3logsq1";

        let message_type = std::fs::read_to_string(&schema_filepath).expect("unable to read parquet schema config");
        let pq_schema = parquet::schema::parser::parse_message_type(&message_type).expect("Expected valid schema");

        // convert parquet schemd to arrow schema
        let schema_desc = parquet::schema::types::SchemaDescriptor::new(Arc::new(pq_schema));
        let schema = parquet::arrow::parquet_to_arrow_schema(&schema_desc, None).expect("unable to convert schema from parquet to arrow");

        let writer_props = ParquetWriterConfigReader::new(&wr_props_fullpath);

        let config = WriterConfig {
            schema_ref: Arc::new(schema),
            writer_props: writer_props,
        };

        let tm = tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current().block_on(async move {
                TransferManager::new(region).await
            })
        });

        Self {
            bucket: bucket.to_string(),
            region: region.to_string(),
            prefix: prefix.to_string(),
            line_parser: LineParser::new(),
            time_partition: TimePartition::new(timezone, hourly_partition),
            config: config,
            chans: Channels::new(),
            tasks: Arc::new(Mutex::new(Vec::new())),
            quit: quit,
            permit: Arc::new(tokio::sync::Semaphore::new(1)),
            tm: tm,
        }
    }

    // read in lines into partitioned fields
    pub async fn lines_to_partition<R>(&self, mut lines: tokio::io::Lines<R>) -> Result<BTreeMap<PartitionedTimeStamp, Vec<LogFields>>>
    where
        R: tokio::io::AsyncBufRead + Unpin
    {
        let mut map: BTreeMap<PartitionedTimeStamp, Vec<LogFields>> = BTreeMap::new();
        let mut stat = TimeStats::new();

        while let Some(line) = lines.next_line().await? {
            let fields = self.line_parser.extract_full(&line, false);
            // requesttime @ fields[2]
            let part_ts = self.time_partition.parse(&fields[2]);
            if let Some(v) = map.get_mut(&part_ts) {
                v.push(fields);
            } else {
                map.insert(part_ts, vec![fields]);
            }
        }

        debug!("lines to v cost: {}", stat.elapsed());
        Ok(map)
    }

    pub async fn _lines_to_v<R>(&self, mut lines: tokio::io::Lines<R>) -> Result<Vec<LogFields>>
    where
        R: tokio::io::AsyncBufRead + Unpin
    {
        let mut v = Vec::new();
        let mut stat = TimeStats::new();

        while let Some(line) = lines.next_line().await? {
            let fields = self.line_parser.extract_full(&line, false);
            v.push(fields);
        }
        debug!("lines to v cost: {}", stat.elapsed());
        Ok(v)
    }

    pub fn lines_to_v_s<S>(&self, lines: std::io::Lines<S>) -> Result<Vec<LogFields>>
    where
        S: std::io::BufRead + Unpin
    {
        let mut stat = TimeStats::new();
        let v = lines.filter_map(|l| {
            let line = self.line_parser.extract_full(&l.unwrap(), false);
            Some(line)
        }).collect::<Vec<LogFields>>();
        debug!("lines to v cost: {}", stat.elapsed());
        Ok(v)
    }

    pub async fn send_by_partition(&self, partition: PartitionedTimeStamp, logs: Vec<LogFields>, receipt: Receipt) {

        if let Some(channel) = self.chans.get(partition).await {
            channel.do_send(logs, receipt).await;
            return;
        }

        // if no channel for this partition found, let's create a new one
        if let Some(channel) = self.safe_start_output_wr(partition).await {
            channel.do_send(logs, receipt).await;
        }
    }

    pub async fn process_s3(&self, sqs_url: &str, client: Arc<Client>, region: &str, bucket: &str, key: &str, sqs_receipt: &str) -> Result<()> {

        let mut stat = TimeStats::new();
        debug!("start to fetch object s3://{}/{} from region: {}", bucket, key, region);

        let tm = TransferManager::new(region).await;
        let stream = self.tm.download_object(bucket, key).await?;

        let lines = BufReader::with_capacity(10*1024*1024, stream.into_async_read()).lines();
        debug!("object s3://{}/{} initialized download cost: {}", bucket, key, stat.elapsed());
        let mut map = self.lines_to_partition(lines).await?;
        /*
        let mut reader = BufReader::with_capacity(10*1024*1024, stream.into_async_read());
        let mut buf = Vec::new();
        let _ = reader.read_to_end(&mut buf).await?;
        debug!("object s3://{}/{} initialized download cost: {}", bucket, key, stat.elapsed());
        let mut cursor = std::io::Cursor::new(buf);
        let v = self.lines_to_v_s(BufRead::lines(cursor)).unwrap();
        */
        let mut receipt_gen = ReceiptGen::new(sqs_url, client, region, bucket, key, sqs_receipt);

        let total = map.len();
        let mut line_start = 0;
        let mut line_count = 0;

        for (partition, logs) in map.into_iter() {

            let mut receipt = receipt_gen.next(total).await;
            line_count = logs.len();
            receipt.finalize(line_start, line_count).await;

            self.send_by_partition(partition, logs, receipt).await;
            line_start += line_count;
        }

        Ok(())
    }

    pub async fn safe_start_output_wr(&self, partition: TimeStamp) -> Option<Channel> {

        if let Ok(permit) = self.permit.acquire().await {
            // check channels before real start a new one
            if let Some(channel) = self.chans.get(partition).await {
                // channel exists, no need to create
                return Some(channel);
            }

            let rx = self.chans.create(partition).await;
            debug!("new channel created for date partition {}", partition);

            let res = self.start_output_wr(partition, rx).await;
            if res.is_err() {
                panic!("failed to start a new output channel task");
                return None;
            }
            // try again
            if let Some(channel) = self.chans.get(partition).await {
                return Some(channel);
            }
        }
        panic!("start output writer semaphore closed!");
    }

    // start a new task for specific date partition
    pub async fn start_output_wr(&self, partition: TimeStamp, rx: Receiver<Vec<LogFields>>) -> Result<()> {

        let quit = self.quit.clone();
        let schema_ref = self.config.schema_ref.clone();
        let writer_props = self.config.writer_props.clone();
        let channels = self.chans.clone();

        let tm = self.tm.clone();

        let join = tokio::task::spawn(async move {

            let mut final_run = false;
            let mut next_rx = rx;
            let mut this_channel: Option<Channel> = None;
            let mut close_channel = false;

            while quit.load(Ordering::SeqCst) != true {

                let ctx = Context {
                    quit: quit.clone(),
                };

                let final_filename = format!("output_{}_{}.parquet", partition, Alphanumeric.sample_string(&mut rand::thread_rng(), 16));
                let incomplete_filename = format!("{}.incomplete", final_filename);

                let parquet_filepath = format!("{}/{}", output_temp_dir, final_filename);
                let incomplete_parquet_filepath = format!("{}/{}", output_temp_dir, incomplete_filename);

                debug!("starting task for output parquet file: {}", incomplete_parquet_filepath);
                let buffer_size = 100 * 1024 * 1024;
                let parquet_file = tokio::fs::File::create(&incomplete_parquet_filepath).await.unwrap();
                let wr = AsyncParquetOutput::new(parquet_file, &incomplete_parquet_filepath, buffer_size, schema_ref.clone(), writer_props.clone(), ctx);

                match wr.output_loop(partition, next_rx.clone(), final_run).await {
                    Ok(Reason::Unkown) => {
                        panic!("[{}] output loop return reason unkown, why?", partition);
                    },
                    Err(e) => {
                        panic!("failed to close parquet output file: {}", e);
                    },
                    Ok(Reason::ChannelDisconnected) => {
                        panic!("[{}] output loop return reason Reason::ChannelDisconnected", partition);
                    },
                    Ok(Reason::Quit) => {
                        debug!("[{}] output loop return reason: Quit", partition);
                        close_channel = true;
                    },
                    Ok(Reason::MaxLinesReached) => {
                        debug!("[{}] output loop return reason: MaxLinesReached", partition);
                        close_channel = false;
                    },
                    Ok(Reason::MaxTimeReached) => {
                        debug!("[{}] output loop return reason: MaxTimeReached", partition);
                        close_channel = false;
                    },
                    Ok(Reason::MaxTimeReachedEmpty) => {
                        let channel = channels.remove(partition).await;
                        if channel.count_sender() > 0 || channel.count_queue() > 0 {
                            // in case their is someone working on this channel or data still on queue,
                            // give this channel last chance to retrieve all data and quit again
                            final_run = true;
                            next_rx = channel.get_rx().clone();
                            this_channel = Some(channel);
                            debug!("[{}] output loop return reason MaxTimeReachedEmpty => need a final run", partition);
                            close_channel = false;
                        } else {
                            debug!("[{}] output loop return reason MaxTimeReachedEmpty => channel is clean, let's quit", partition);
                            this_channel = Some(channel);
                            close_channel = true;
                        }
                    },
                    Ok(Reason::Final) => {
                        debug!("[{}] output loop return reason Final => time to close this channel", partition);
                        assert!(next_rx.len() == 0);
                        assert!(final_run == true);
                        close_channel = true;
                    },
                }

                debug!("[{}] before channel close", partition);
                // 1. rename output file to final name
                tokio::fs::rename(&incomplete_parquet_filepath, &parquet_filepath)
                    .await
                    .map_err(|e| {
                        error!("failed to rename {} to {}", incomplete_parquet_filepath, parquet_filepath);
                        panic!("unable to rename file");
                    });
                // 2. upload to S3
                let key = format!("stream/{}", final_filename);
                let res = tm.upload_object(&parquet_filepath, "ahaparquet", &key, 0).await;
                if res.is_ok() {
                    tokio::fs::remove_file(&parquet_filepath)
                        .await
                        .map_err(|e| {
                            error!("failed to remove {}", parquet_filepath);
                            panic!("unable to remove file");
                        });
                } else {
                    panic!("failed to upload final output to s3");
                }


                if close_channel == false {
                    // just rotate output file, don't close channel
                    continue;
                }

                // 3. close channel and callback all token
                if let Some(channel) = this_channel {
                    channel.close().await;
                }
                debug!("[{}] before channel closed", partition);
                break;
            }
            ()
        });

/*
        let mut tasks = self.tasks.lock().await;
        tasks.push(join);
        */
        Ok(())
    }
}

#[derive(Debug)]
enum Reason {
    Unkown,
    MaxLinesReached,
    MaxTimeReached,
    MaxTimeReachedEmpty,
    Final,
    ChannelDisconnected,
    Quit,
}

pub struct AsyncParquetOutput<W> {
    writer: AsyncArrowWriter<W>,
    schema_ref: SchemaRef,
    ctx: Context,
    max_fields: usize,
    file_path: String,
}

impl<W: AsyncWrite + Unpin + Send> AsyncParquetOutput<W> {

    pub fn new(buf_wr: W, file_path: &str, buffer_size: usize, schema_ref: SchemaRef, writer_props: WriterProperties, ctx: Context) -> Self {

        let max = schema_ref.fields.len();

        // build writer
        let writer = AsyncArrowWriter::try_new(buf_wr, schema_ref.clone(), buffer_size, Some(writer_props.clone())).unwrap();

        Self {
            writer: writer,
            schema_ref: schema_ref,
            ctx: ctx,
            max_fields: max,
            file_path: file_path.to_string(),
        }
    }

    pub async fn output_loop(mut self, partition: PartitionedTimeStamp, output_channel: Receiver<Vec<LogFields>>, final_run: bool) -> Result<Reason> {

        debug!("[{}] output_loop started, is final_run {}", partition, final_run);
        let mut reason = Reason::Unkown;
        let mut lines_written = 0;
        let mut last_activity = std::time::SystemTime::now();
        let mut total = TimeStats::new();

        while self.ctx.quit.load(Ordering::SeqCst) != true {

            match output_channel.try_recv() {
                Ok(lines) => {
                    let count = lines.len();
                    self.append_lines(lines).await;
                    lines_written += count;
                    if lines_written >= output_threshold_lines && !final_run {
                        reason = Reason::MaxLinesReached;
                        break;
                    }
                    last_activity = std::time::SystemTime::now();
                    reason = Reason::Quit;
                    continue;
                },
                Err(crossbeam::channel::TryRecvError::Empty) => {
                    if final_run {
                        reason = Reason::Final;
                        break;
                    }
                    if last_activity.elapsed().unwrap() >= std::time::Duration::new(output_threshold_maxidle, 0) {
                        if output_channel.len() == 0 {
                            reason = Reason::MaxTimeReachedEmpty;
                        } else {
                            reason = Reason::MaxTimeReached;
                        }
                        break;
                    }
                    reason = Reason::Quit;
                    continue;
                },
                Err(crossbeam::channel::TryRecvError::Disconnected) => {
                    debug!("[{}] channel disconnected, {} lines written, is final_run {}", partition, lines_written, final_run);
                    if final_run {
                        reason = Reason::Final;
                        break;
                    }
                    reason = Reason::ChannelDisconnected;
                    break;
                }
            }
        }

        debug!("[{}] closing parquet file before quit, pls be patient {}, is final_run {}", partition, self.file_path, final_run);
        let mut stats = TimeStats::new();
        let res = self.writer.close().await;
        if res.is_err() {
            panic!("[{}] failed to gen output parquet file {} err: {:?}", partition, self.file_path, res);
        }
        debug!("[{}] closing parquet {} cost: {}, is final_run {}", partition, self.file_path, stats.elapsed(), final_run);
        debug!("[{}] output_loop return, reason: {:?}, is final_run {}", partition, reason, final_run);
        info!("[{}] {} lines written to parquet file {} cost {}", partition, lines_written, self.file_path, total.elapsed());

        Ok(reason)
    }

    pub fn vec_to_columns(&self, v: Vec<LogFields>) -> ArrowResult<Vec<ArrayRef>> {
        let null = "".to_string();
        let mut stat = TimeStats::new();
        let arrays: ArrowResult<Vec<ArrayRef>> = (0..self.max_fields)
                                                .map(|idx| {
                                                    Ok(Arc::new(v.iter()
                                                            .map(|row| row.get(idx).or(Some(&null)))
                                                            .collect::<StringArray>(),) as ArrayRef)
                                                })
                                                .collect();
        debug!("fields to columns cost: {}", stat.elapsed());
        arrays
    }

    pub async fn append_lines(&mut self, v: Vec<LogFields>) -> Result<()> {

        // FIXME
        let columns = self.vec_to_columns(v).unwrap();
        let mut stat = TimeStats::new();
        let batch = RecordBatch::try_new(Arc::clone(&self.schema_ref), columns).unwrap();
        debug!("columes to recordbatch cost: {}", stat.elapsed());
        let mut stat = TimeStats::new();
        let res = self.writer.write(&batch).await;
        debug!("write to parquet cost: {}", stat.elapsed());
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
