use std::path::Path;
use std::env;
use std::fmt;
use std::sync::Arc;
use std::io::{BufRead, Write};
use std::time::{SystemTime, Duration, Instant};
use std::pin::Pin;
use std::task::Poll;
use std::future::Future;
use std::os::unix::fs::MetadataExt;
use std::collections::{HashMap, HashSet};
use log::{info, debug, warn, error};
use tokio::fs:: OpenOptions;
use tokio::io::{BufReader, AsyncBufReadExt};
use tokio::io:: AsyncWriteExt;
use tokio::io::{Error, ErrorKind};
use tokio::io::Lines;
use pcre2::bytes::{RegexBuilder, Regex};
use chrono::prelude::*;
use fs4::tokio::AsyncFileExt;
use fs4::FileExt;
use flate2::GzBuilder;
use rand::distributions::{Alphanumeric, DistString};
use concat_reader::concat_path;

use arrow::array::{StringArray, ArrayRef};
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use arrow::datatypes::{DataType, Field, Schema};

use crate::transfer::TransferManager;
use crate::stats::TimeStats;
use crate::conf::ParquetWriterConfigReader;

type S3LogLine = String;
type TimeStamp = usize;
type OrigBucket = String;

// what S3 default
const S3_LOG_REGEX_FULL: &str = r#"\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}\ \+\d{4})\]|"([^"]+)"|([^\s""\[\]]+)"#;
const S3_LOG_REGEX_TIMESTAMP: &str = r#"(\S+) (\S+) \[([^\]]+)\]"#;
const S3_LOG_REGEX_ORIG_BUCKET: &str = r#"(\S+) (\S+)"#;
const S3_LOG_DATATIME_FMT: &str = "%d/%b/%Y:%H:%M:%S %z";

const S3LOGS_CONFIG_DEFAULT_ROOT_PATH: &str = "/mnt/s3logs/config";
const S3LOGS_CONFIG_DEFAULT_PARQUET_SCHEMA_FILE: &str = "parquet.schema";
const S3LOGS_CONFIG_DEFAULT_PARQUET_WRITER_PROPERTIES_FILE: &str = "parquet_writer_properties.ini";

const S3LOGS_STAGGING_DEFAULT_PARTITION_SECOND: usize = 900;
const S3LOGS_STAGGING_DEFAULT_PARTITION_TZIF: &str = "UTC+0";
const S3LOGS_STAGGING_DEFAULT_ROOT_PATH: &str = "/mnt/s3logs/stagging";
const S3LOGS_STAGGING_DEFAULT_FILE_DATETIME_FMT: &str = "%Y-%m-%d-%H-%M-%S%z";
const S3LOGS_STAGGING_DEFAULT_FILE_SUFFIX: &str = ".s3logs";
const S3LOGS_STAGGING_DEFAULT_PROCESSING_SUFFIX: &str = ".processing";
const S3LOGS_STAGGING_DEFAULT_MERGE_ORIG_BUCKETS: bool = true;
const S3LOGS_STAGGING_DEFAULT_FILE_LOCK_TIMEOUT_SECONDS: u64 = 30;
const S3LOGS_STAGGING_DEFAULT_FILE_LOCK_RETRY_WAIT_MS: u64 = 100;
const S3LOGS_STAGGING_FILE_REGEX: &str = r#"([^_]+)_(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}\+\d{4})(\.[0-9a-zA-Z]+)(\.[0-9a-zA-Z]+){0,1}"#;
const S3LOGS_STAGGING_ALL_BUCKETS: &str = "ALLBUCKETS";

const S3LOGS_TRANSFORM_DEFAULT_ARCHIVE_ROOT_PATH: &str = "/mnt/s3logs/archive";
const S3LOGS_TRANSFORM_DEFAULT_PARQUET_ROOT_PATH: &str = "/mnt/s3logs/parquet";
const S3LOGS_TRANSFORM_DEFAULT_OUTPUT_PREFIX_FMT: &str = "year=%Y/month=%m/day=%d/hour=%H";
const S3LOGS_TRANSFORM_DEFAULT_OUTPUT_TARGET_PREFIX: &str = "";
const S3LOGS_TRANSFORM_DEFAULT_AGGREGATE_SECOND: usize = 900;
const S3LOGS_TRANSFORM_DEFAULT_LOG_DEDUPLICATION: bool = true;
const S3LOGS_TRANSFORM_DEFAULT_CLEANUP_PROCESSED_LOGS: bool = true;
const S3LOGS_TRANSFORM_DEFAULT_CLEANUP_UPLOADED_PARQUET: bool = true;
// 600B per log line, it's about 114MB
const S3LOGS_TRANSFORM_DEFAULT_PARQUET_WRTIER_BULK_LINES: usize = 200000;
// 10 min
const S3LOGS_TRANSFORM_DEFAULT_JOB_INTERVAL: u64 = 600;

// 100 MiB
const S3LOGS_FILE_DEFAULT_BUF_SIZE: usize = 104857600;

struct FileLock<'a> {
    file: &'a tokio::fs::File,
    start: Instant,
    timeout: Duration,
}

impl<'a> FileLock<'a> {
    fn new(file: &'a tokio::fs::File, timeout: Duration) -> Self {
        Self {
            file: file,
            start: Instant::now(),
            timeout: timeout,
        }
    }
}

impl<'a> Future for FileLock<'a> {
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut core::task::Context<'_>) -> Poll<Self::Output> {

        match self.file.try_lock_exclusive() {
            Ok(_) => {
                return Poll::Ready(Ok(()));
            },
            Err(e) => {
                if e.kind() != ErrorKind::WouldBlock {
                    warn!("unhandled error occur when try lock exlusive: {}", e);
                    return Poll::Ready(Err(Error::new(ErrorKind::Other, "try lock exlusive failed")));
                }
            },
        }

        if self.start.elapsed() >= self.timeout {
            return Poll::Ready(Err(Error::new(ErrorKind::TimedOut, "timeout")));
        }

        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

#[inline]
async fn backoff_lock_try(file: &tokio::fs::File, filename: &str, max_spin: u64, backoff_step_ms: u64, timeout: Duration) -> Result<(), Error> {

    let mut spin_count = 1;
    let mut sleep_count = 0;
    let start = Instant::now();

    loop {
        match file.try_lock_exclusive() {
            Ok(_) => {
                debug!("spin stat - spin count: {}, sleep count: {}, total cost: {:#?}", spin_count, sleep_count, start.elapsed());
                return Ok(());
            },
            Err(e) => {
                if e.kind() != ErrorKind::WouldBlock {
                    warn!("unhandled error occur when try lock exlusive: {}", e);
                    panic!("unhandled error occur when try lock exlusive: {}", e);
                };
            },
        };

        spin_count += 1;
        if spin_count % max_spin == 0 {
            if start.elapsed() >= timeout {
                warn!("try_lock_exclusive timeout - file: {}, spin count: {}, sleep count: {}, total cost: {:#?}",
                    filename, spin_count, sleep_count, start.elapsed());
                return Err(Error::new(ErrorKind::TimedOut, "timeout"));
            }
            sleep_count += 1;
            tokio::time::sleep(tokio::time::Duration::from_millis(sleep_count*backoff_step_ms)).await;
        }
    }
}

fn lock_exclusive_try(file: &std::fs::File, timeout: Duration, retry_wait: Duration) -> Result<(), Error> {

    let mut total = Duration::new(0, 0);
    loop {
        match file.try_lock_exclusive() {
            Err(e) => {
                if e.kind() != ErrorKind::WouldBlock {
                    warn!("unhandled error occur when try lock exlusive: {}", e);
                    return Err(Error::new(ErrorKind::Other, "try lock exlusive failed"));
                }
                if total > timeout {
                    return Err(Error::new(ErrorKind::TimedOut, "lock exlusive timeout"));
                }
                std::thread::sleep(retry_wait);
                total += retry_wait;
            },
            Ok(_) => {
                return Ok(());
            },
        }
    }
}

fn set_mtime(file_path: &str, mtime: i64) -> Result<(), Error> {
    use libc::{c_char, c_int, utimbuf, time_t};
    use std::ffi::CString;

    extern "C" {
        fn utime(filename: *const c_char, times: *const utimbuf) -> c_int;
    }

    let path = CString::new(file_path).unwrap();

    let ut = utimbuf {
        actime: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as time_t,
        modtime: mtime as time_t,
    };

    let ret = unsafe {
        utime(path.as_ptr(), &ut as *const utimbuf)
    };
    if ret == 0 {
        Ok(())
    } else {
        Err(Error::last_os_error())
    }
}

#[derive(Clone)]
pub struct LineParser {
    re_timestamp: Regex,
    re_orig_bucket: Regex,
    re_full: Regex,
}

impl LineParser {

    fn new() -> Self {
        Self {
            re_timestamp: RegexBuilder::new()
                .jit(true)
                .build(S3_LOG_REGEX_TIMESTAMP)
                .unwrap(),
            re_orig_bucket: RegexBuilder::new()
                .jit(true)
                .build(S3_LOG_REGEX_ORIG_BUCKET)
                .unwrap(),
            re_full: RegexBuilder::new()
                .jit(true)
                .build(S3_LOG_REGEX_FULL)
                .unwrap(),
        }
    }

    fn timestamp_align_left(ts: TimeStamp, align: usize) -> TimeStamp {
        ts - ts % align
    }

    fn timestamp_align_right(ts: TimeStamp, align: usize) -> TimeStamp {
        ts - ts % align + align
    }

    #[allow(dead_code)]
    fn timestamp_to_fmt(ts: TimeStamp, tz: FixedOffset, fmt: &str) -> String {
        let dt = tz.timestamp_opt(ts as i64, 0).unwrap();
        dt.format(fmt).to_string()
    }

    #[allow(dead_code)]
    fn timestamp_to_fmt_utc(ts: TimeStamp, fmt: &str) -> String {
        let dt = Utc.timestamp_opt(ts as i64, 0).unwrap();
        dt.format(fmt).to_string()
    }

    pub fn extract_ts(&self, line: &str) -> Option<TimeStamp> {

        let caps = self.re_timestamp.captures(line.as_bytes()).unwrap();

        let m = caps.and_then(|cap| cap.get(3));
        if m.is_none() {
            return None;
        }

        let ts_str = std::str::from_utf8(m.unwrap().as_bytes()).unwrap();
        DateTime::parse_from_str(ts_str, S3_LOG_DATATIME_FMT)
            .ok()
            .and_then(|dt| Some(dt.timestamp() as TimeStamp))
    }

    // we don't need handle parse error here because extract_ts happens before this.
    // if 3 groups captured in extract_ts, we have the confidence to get 2nd group
    pub fn extract_bucket(&self, line: &str) -> String {

        let caps = self.re_orig_bucket.captures(line.as_bytes()).unwrap();

        String::from_utf8(caps.unwrap().get(2).unwrap().as_bytes().to_vec()).unwrap()
    }

    pub fn extract_full(&self, line: &str, replace: bool) -> Vec<String> {

        let mut v = Vec::new();

        for (index, m) in self.re_full.captures_iter(line.as_bytes()).enumerate() {
            let m1 = m.as_ref().unwrap().get(1);
            let m2 = m.as_ref().unwrap().get(2);
            let m3 = m.as_ref().unwrap().get(3);
            let field = [m1, m2, m3].into_iter().find(|i| i.is_some());
            if index == 2 && replace {
                let dt = DateTime::parse_from_str(
                    std::str::from_utf8(field.unwrap().unwrap().as_bytes()).unwrap(), S3_LOG_DATATIME_FMT).unwrap();
                v.push(dt.timestamp().to_string());
            } else {
                v.push(String::from_utf8(field.unwrap().unwrap().as_bytes().to_vec()).unwrap());
            }
        }
        v
    }
}

#[derive(Debug)]
pub struct StaggingFile {
    root_path: String,
    archive_root_path: String,
    orig_bucket: String,
    datetime: String,
    stagging_suffix: String,
    processing_suffix: String,
    stagging_dt_fmt: String,
    processing: bool,
    valid: bool,
}

impl fmt::Display for StaggingFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.get_fullpath())
    }
}

impl StaggingFile {

    pub fn new(orig_bucket: &str, datetime: &str) -> Self {
        Self {
            root_path: env::var("S3LOGS_STAGGING_ROOT_PATH")
                            .unwrap_or(S3LOGS_STAGGING_DEFAULT_ROOT_PATH.to_string()),
            archive_root_path: env::var("S3LOGS_TRANSFORM_ARCHIVE_ROOT_PATH")
                            .unwrap_or(S3LOGS_TRANSFORM_DEFAULT_ARCHIVE_ROOT_PATH.to_string()),
            orig_bucket: orig_bucket.to_string(),
            datetime: datetime.to_string(),
            stagging_suffix: env::var("S3LOGS_STAGGING_FILE_SUFFIX")
                            .unwrap_or(S3LOGS_STAGGING_DEFAULT_FILE_SUFFIX.to_string()),
            processing_suffix: env::var("S3LOGS_STAGGING_PROCESSING_SUFFIX")
                            .unwrap_or(S3LOGS_STAGGING_DEFAULT_PROCESSING_SUFFIX.to_string()),
            stagging_dt_fmt: env::var("S3LOGS_STAGGING_FILE_DATETIME_FMT")
                            .unwrap_or(S3LOGS_STAGGING_DEFAULT_FILE_DATETIME_FMT.to_string()),
            processing: false,
            valid: false,
        }
    }

    pub fn self_copy(&self) -> Self {
        Self {
            root_path: self.root_path.clone(),
            archive_root_path: self.archive_root_path.clone(),
            orig_bucket: self.orig_bucket.clone(),
            datetime: self.datetime.clone(),
            stagging_suffix: self.stagging_suffix.clone(),
            processing_suffix: self.processing_suffix.clone(),
            stagging_dt_fmt: self.stagging_dt_fmt.clone(),
            processing: self.processing,
            valid: self.valid,
        }
    }

    pub fn new_from_ts(orig_bucket: &str, tz: FixedOffset, ts: TimeStamp) -> Self {
        let dt = tz.timestamp_opt(ts as i64, 0).unwrap();
        let stagging_dt_fmt = env::var("S3LOGS_STAGGING_FILE_DATETIME_FMT")
                            .unwrap_or(S3LOGS_STAGGING_DEFAULT_FILE_DATETIME_FMT.to_string());
        let datetime = format!("{}", dt.format(&stagging_dt_fmt));
        Self::new(orig_bucket, &datetime)
    }

    pub fn new_from_filename(filename: &str) -> Self {
        let re = regex::Regex::new(S3LOGS_STAGGING_FILE_REGEX).unwrap();

        let res = re.captures(filename);
        if res.is_none() {
            return Self::new("", "");
        }

        let caps = res.unwrap();

        let orig_bucket = caps.get(1).unwrap().as_str();
        let datetime = caps.get(2).unwrap().as_str();

        let mut s = Self::new(orig_bucket, datetime);

        let mut suffix = caps.get(3).unwrap().as_str();
        if suffix != s.stagging_suffix() {
            // a invalid filename
            return s;
        }

        if caps.get(4).is_some() {
            suffix =  caps.get(4).unwrap().as_str();
            if suffix == s.processing_suffix() {
                s.set_processing();
            } else {
                // a invalid filename
                return s;
            }
        }
        s.set_valid(true);
        s
    }

    pub fn new_from_fullpath(fullpath: &str) -> Self {

        let mut s: Vec<&str> = fullpath.split("/").collect();
        let filename = s.pop().unwrap_or_default();
        Self::new_from_filename(filename)
    }

    pub fn get_fullpath(&self) -> String {
        format!("{}/{}_{}{}", self.root_path, self.orig_bucket, self.datetime, self.stagging_suffix)
    }

    pub fn get_processing_fullpath(&self) -> String {
        format!("{}/{}_{}{}{}", self.root_path, self.orig_bucket, self.datetime, self.stagging_suffix, self.processing_suffix)
    }

    pub fn get_archive_fullpath(&self) -> String {
        let dtstr = Local::now().format(&self.stagging_dt_fmt);
        format!("{}/{}_{}{}.{}", self.archive_root_path, self.orig_bucket, self.datetime, self.stagging_suffix, dtstr)
    }

    pub fn is_processing(&self) -> bool {
        self.processing && self.valid
    }

    pub fn is_stagging(&self) -> bool {
        !self.processing && self.valid
    }

    pub fn set_processing(&mut self) {
        self.processing = true;
    }

    pub fn set_valid(&mut self, valid: bool) {
        self.valid = valid
    }

    pub fn is_valid(&self) -> bool {
        self.valid
    }

    pub fn processing_suffix(&self) -> &str {
        &self.processing_suffix
    }

    pub fn stagging_suffix(&self) -> &str {
        &self.stagging_suffix
    }

    pub fn orig_bucket(&self) -> &str {
        &self.orig_bucket
    }

    pub fn datetime(&self) -> &str {
        &self.datetime
    }

    pub fn datetime_ts(&self) -> TimeStamp {
        let res = DateTime::parse_from_str(&self.datetime, &self.stagging_dt_fmt);
        if res.is_err() {
            return 0;
        }
        res.unwrap().timestamp() as TimeStamp
    }
}

pub struct S3LogAggregator {
    bucket: String,
    key: String,
    region: String,
    tz: FixedOffset,
    stagging_root: String,
    stagging_dt_fmt: String,
    stagging_suffix: String,
    stagging_second: usize,
    file_buf_size: usize,
    merge_orig_buckets: bool,
    line_parser: LineParser,
    flock_timeout: Duration,
    flock_retry_wait: Duration,
}

impl S3LogAggregator {

    pub fn new(region: &str, bucket: &str, key: &str,
            tz: Option<&str>, root: Option<&str>,
            fmt: Option<&str>, second: Option<usize>) -> Self {

        let utc_offset;
        if tz.is_some() {
            let ptz = tzif::parse_posix_tz_string(tz.unwrap().as_bytes())
                    .unwrap_or(tzif::parse_posix_tz_string(S3LOGS_STAGGING_DEFAULT_PARTITION_TZIF.as_bytes()).unwrap());
            utc_offset = ptz.std_info.offset.0;
        } else {
            let tzstr = env::var("S3LOGS_STAGGING_PARTITION_TZIF")
                    .unwrap_or(S3LOGS_STAGGING_DEFAULT_PARTITION_TZIF.to_string());
            let ptz = tzif::parse_posix_tz_string(tzstr.as_bytes())
                    .unwrap_or(tzif::parse_posix_tz_string(S3LOGS_STAGGING_DEFAULT_PARTITION_TZIF.as_bytes()).unwrap());
            utc_offset = ptz.std_info.offset.0;
        }

        Self {
            bucket: bucket.to_string(),
            key: key.to_string(),
            region: region.to_string(),
            tz: FixedOffset::east_opt(utc_offset as i32).unwrap(),
            stagging_root: root.or(
                    Some(&env::var("S3LOGS_STAGGING_ROOT_PATH")
                        .unwrap_or(S3LOGS_STAGGING_DEFAULT_ROOT_PATH.to_string()))
                ).unwrap().to_string(),
            stagging_dt_fmt: fmt.or(
                    Some(&env::var("S3LOGS_STAGGING_FILE_DATETIME_FMT")
                        .unwrap_or(S3LOGS_STAGGING_DEFAULT_FILE_DATETIME_FMT.to_string()))
                ).unwrap().to_string(),
            stagging_suffix: fmt.or(
                    Some(&env::var("S3LOGS_STAGGING_FILE_SUFFIX")
                        .unwrap_or(S3LOGS_STAGGING_DEFAULT_FILE_SUFFIX.to_string()))
                ).unwrap().to_string(),
            stagging_second: second.or(
                    Some(env::var("S3LOGS_STAGGING_PARTITION_SECOND")
                        .map_or(S3LOGS_STAGGING_DEFAULT_PARTITION_SECOND,
                                |x| x.parse::<usize>()
                        .unwrap_or(S3LOGS_STAGGING_DEFAULT_PARTITION_SECOND)))
                ).unwrap(),
            file_buf_size: env::var("S3LOGS_FILE_BUF_SIZE")
                    .map_or(S3LOGS_FILE_DEFAULT_BUF_SIZE,
                            |x| x.parse::<usize>()
                    .unwrap_or(S3LOGS_FILE_DEFAULT_BUF_SIZE)),
            merge_orig_buckets: env::var("S3LOGS_STAGGING_MERGE_ORIG_BUCKETS")
                    .map_or(S3LOGS_STAGGING_DEFAULT_MERGE_ORIG_BUCKETS,
                            |x| x.parse::<bool>()
                    .unwrap_or(S3LOGS_STAGGING_DEFAULT_MERGE_ORIG_BUCKETS)),
            line_parser: LineParser::new(),
            flock_timeout: Duration::from_secs(
                        env::var("S3LOGS_STAGGING_FILE_LOCK_TIMEOUT_SECONDS")
                            .map_or(S3LOGS_STAGGING_DEFAULT_FILE_LOCK_TIMEOUT_SECONDS,
                                    |x| x.parse::<u64>()
                            .unwrap_or(S3LOGS_STAGGING_DEFAULT_FILE_LOCK_TIMEOUT_SECONDS))
                        ),
            flock_retry_wait: Duration::from_millis(
                        env::var("S3LOGS_STAGGING_FILE_LOCK_RETRY_WAIT_MS")
                            .map_or(S3LOGS_STAGGING_DEFAULT_FILE_LOCK_RETRY_WAIT_MS,
                                    |x| x.parse::<u64>()
                            .unwrap_or(S3LOGS_STAGGING_DEFAULT_FILE_LOCK_RETRY_WAIT_MS))
                        ),
        }
    }

    pub fn self_copy(&self) -> Self {
        Self {
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            region: self.region.clone(),
            tz: self.tz,
            stagging_root: self.stagging_root.clone(),
            stagging_dt_fmt: self.stagging_dt_fmt.clone(),
            stagging_suffix: self.stagging_suffix.clone(),
            stagging_second: self.stagging_second,
            file_buf_size: self.file_buf_size,
            merge_orig_buckets: self.merge_orig_buckets,
            line_parser: self.line_parser.clone(),
            flock_timeout: self.flock_timeout.clone(),
            flock_retry_wait: self.flock_retry_wait.clone(),
        }
    }

    fn ts_to_full_stagging_path(&self, orig_bucket: &str, ts: TimeStamp) -> String {
        StaggingFile::new_from_ts(orig_bucket, self.tz, ts).get_fullpath()
    }

    pub async fn process_s3(&self) -> Result<usize, Error> {

        let mut stat = TimeStats::new();
        info!("start to fetch object s3://{}/{} from region: {}", self.bucket, self.key, self.region);

        let tm = TransferManager::new(&self.region).await;
        let stream = tm.download_object(&self.bucket, &self.key).await?;
        let lines = BufReader::new(stream.into_async_read()).lines();
        info!("object s3://{}/{} initialized download cost: {}", self.bucket, self.key, stat.elapsed());

        let total = self.process(lines).await?;
        Ok(total)
    }

    pub async fn process_local(&self, filename: &str) -> Result<usize, Error> {

        let ifile = tokio::fs::File::open(filename).await?;
        info!("start to process local file {}", filename);

        let lines = BufReader::with_capacity(self.file_buf_size, ifile).lines();

        let total = self.process(lines).await?;
        Ok(total)
    }

    /*
     * @return - number of lines processed
     */
    async fn process<R>(&self, mut lines: Lines<R>) -> Result<usize, Error>
    where
        R: tokio::io::AsyncBufRead + Unpin
    {

        let mut stat = TimeStats::new();

        let mut logs: Vec<(TimeStamp, S3LogLine)> = Vec::new();

        while let Some(line) = lines.next_line().await? {

            // skip lines if parse failed
            if let Some(ts) = self.line_parser.extract_ts(&line) {
                logs.push((ts, line.to_string()));
            }
        }

        let totals = logs.len();
        info!("total {} lines fetched, cost: {}", totals, stat.elapsed());

        logs.sort_by_key(|k| k.0);
        info!("sort lines, cost: {}", stat.elapsed());

        let first = logs.first().unwrap().0;
        let last = logs.last().unwrap().0;

        let align_first = LineParser::timestamp_align_right(first, self.stagging_second);
        let align_last = LineParser::timestamp_align_right(last, self.stagging_second);

        let mut partitioned = Vec::new();
        let mut next_bound = align_first;

        while next_bound <= align_last {
            let mut iter = logs.iter();

            let idx = iter.position(|x| x.0 >= next_bound);
            next_bound += self.stagging_second;

            if idx.is_none() {
                continue;
            }

            let logs_left = logs.split_off(idx.unwrap());
            partitioned.push(logs);
            logs = logs_left;
        }
        info!("logs partitioned, cost: {}", stat.elapsed());

        // aways push last one
        partitioned.push(logs);

        // remove any 0 size sub vec
        partitioned.retain(|x| x.len() > 0);

        // check count consistency
        let sum = partitioned.iter().fold(0, |acc, v| acc + v.len());
        assert!(totals == sum, "final entries incorrect, totals: {} - final: {}", totals, sum);

        info!("finally we got {} of partitions, {} of log lines ready for stagging dispatch", partitioned.len(), sum);

        let total = self.dispatch(partitioned).await?;

        Ok(total)
    }

    /*
     * @return - number of lines processed
     */
    #[allow(dead_code)]
    pub async fn _dispatch(&self, parts: Vec<Vec<(TimeStamp, S3LogLine)>>) -> Result<usize, Error> {

        let files = parts.len();

        let mut joins = Vec::new();
        for part in parts {
            let me = Self::self_copy(self);
            let join = tokio::spawn(async move {
                me.task_append_stagging(part).await
            });
            joins.push(join);
        }

        let mut total = 0;
        for join in joins {
            let count = join.await??;
            total += count;
        }

        info!("total {} of lines appending to {} stagging log file", total, files);
        Ok(total)
    }

    pub async fn dispatch(&self, parts: Vec<Vec<(TimeStamp, S3LogLine)>>) -> Result<usize, Error> {

        let files = parts.len();

        let mut total = 0;
        for part in parts {
            let count = self.task_append_stagging(part).await?;
            total += count;
        }

        info!("total {} of lines appending to {} stagging log file", total, files);
        Ok(total)
    }

    pub async fn task_append_stagging(&self, lines: Vec<(TimeStamp, S3LogLine)>) -> Result<usize, Error> {

        let sz = lines.len();
        let first = lines.first().unwrap();
        let mut orig_bucket = self.line_parser.extract_bucket(&first.1);
        if self.merge_orig_buckets {
            orig_bucket = S3LOGS_STAGGING_ALL_BUCKETS.to_string();
        }
        let ts = LineParser::timestamp_align_left(first.0, self.stagging_second);
        let path_str = self.ts_to_full_stagging_path(&orig_bucket, ts);
        let path = Path::new(&path_str);

        let mut file = OpenOptions::new()
                            .write(true)
                            .create(true)
                            .append(true)
                            .open(&path)
                            .await
                            .map_err(|e| {
                                warn!("failed to open file {} , err: {}", path.display(), e);
                                e
                            })?;


        let (_, logs): (Vec<_>, Vec<_>) = lines.iter().cloned().unzip();

        let mut stat = TimeStats::new();
        info!("trying excl lock for file {}", path.as_os_str().to_str().unwrap());
        if true {
            backoff_lock_try(&file, path.as_os_str().to_str().unwrap(), 10, 25, self.flock_timeout).await?;
        } else {
            let flock = FileLock::new(&file, self.flock_timeout);
            flock.await?;
        }
        info!("file {} locked", path.as_os_str().to_str().unwrap());

        file.write_all((logs.join("\n") + "\n").as_bytes()).await?;
        file.flush().await?;

        file.unlock()?;
        info!("file {} unlocked, {} lines appended, cost: {}", path.as_os_str().to_str().unwrap(), sz, stat.elapsed());
        Ok(sz)
    }
}

pub struct S3LogTransform {
    bucket: String,
    region: String,
    target_prefix: String,
    tz: FixedOffset,
    stagging_root: String,
    stagging_dt_fmt: String,
    stagging_suffix: String,
    parquet_root: String,
    prefix_fmt: String,
    schema: Schema,
    job_interval: u64,
    aggregate_second: usize,
    dedup_enabled: bool,
    cleanup_processed_logs: bool,
    cleanup_uploaded_parquet: bool,
    file_buf_size: usize,
    writer_bulk_lines: usize,
    writer_props: WriterProperties,
    line_parser: LineParser,
    flock_timeout: Duration,
    flock_retry_wait: Duration,
}

impl S3LogTransform {

    pub fn new(region: &str, bucket: &str,
            tz: Option<&str>, root: Option<&str>,
            fmt: Option<&str>) -> Self {

        let utc_offset;
        if tz.is_some() {
            let ptz = tzif::parse_posix_tz_string(tz.unwrap().as_bytes())
                    .unwrap_or(tzif::parse_posix_tz_string(S3LOGS_STAGGING_DEFAULT_PARTITION_TZIF.as_bytes()).unwrap());
            utc_offset = ptz.std_info.offset.0;
        } else {
            let tzstr = env::var("S3LOGS_STAGGING_PARTITION_TZIF")
                    .unwrap_or(S3LOGS_STAGGING_DEFAULT_PARTITION_TZIF.to_string());
            let ptz = tzif::parse_posix_tz_string(tzstr.as_bytes())
                    .unwrap_or(tzif::parse_posix_tz_string(S3LOGS_STAGGING_DEFAULT_PARTITION_TZIF.as_bytes()).unwrap());
            utc_offset = ptz.std_info.offset.0;
        }

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

        let parquet_writer_props_file = env::var("S3LOGS_CONFIG_PARQUET_WRITER_PROPERTIES_FILE")
                            .unwrap_or(S3LOGS_CONFIG_DEFAULT_PARQUET_WRITER_PROPERTIES_FILE.to_string());
        let wr_props_fullpath = format!("{}/{}", config_root, parquet_writer_props_file);
        let writer_props = ParquetWriterConfigReader::new(&wr_props_fullpath);

        Self {
            bucket: bucket.to_string(),
            region: region.to_string(),
            target_prefix: env::var("S3LOGS_TRANSFORM_OUTPUT_TARGET_PREFIX")
                    .unwrap_or(S3LOGS_TRANSFORM_DEFAULT_OUTPUT_TARGET_PREFIX.to_string()),
            tz: FixedOffset::east_opt(utc_offset as i32).unwrap(),
            stagging_root: root.or(
                    Some(&env::var("S3LOGS_STAGGING_ROOT_PATH")
                        .unwrap_or(S3LOGS_STAGGING_DEFAULT_ROOT_PATH.to_string()))
                ).unwrap().to_string(),
            stagging_dt_fmt: fmt.or(
                    Some(&env::var("S3LOGS_STAGGING_FILE_DATETIME_FMT")
                        .unwrap_or(S3LOGS_STAGGING_DEFAULT_FILE_DATETIME_FMT.to_string()))
                ).unwrap().to_string(),
            stagging_suffix: fmt.or(
                    Some(&env::var("S3LOGS_STAGGING_FILE_SUFFIX")
                        .unwrap_or(S3LOGS_STAGGING_DEFAULT_FILE_SUFFIX.to_string()))
                ).unwrap().to_string(),
            parquet_root: fmt.or(
                    Some(&env::var("S3LOGS_TRANSFORM_PARQUET_ROOT_PATH")
                        .unwrap_or(S3LOGS_TRANSFORM_DEFAULT_PARQUET_ROOT_PATH.to_string()))
                ).unwrap().to_string(),
            prefix_fmt: fmt.or(
                    Some(&env::var("S3LOGS_TRANSFORM_OUTPUT_PREFIX_FMT")
                        .unwrap_or(S3LOGS_TRANSFORM_DEFAULT_OUTPUT_PREFIX_FMT.to_string()))
                ).unwrap().to_string(),
            schema: schema,
            job_interval: env::var("S3LOGS_TRANSFORM_JOB_INTERVAL")
                    .map_or(S3LOGS_TRANSFORM_DEFAULT_JOB_INTERVAL,
                            |x| x.parse::<u64>()
                    .unwrap_or(S3LOGS_TRANSFORM_DEFAULT_JOB_INTERVAL)),
            aggregate_second: env::var("S3LOGS_TRANSFORM_AGGREGATE_SECOND")
                    .map_or(S3LOGS_TRANSFORM_DEFAULT_AGGREGATE_SECOND,
                            |x| x.parse::<usize>()
                    .unwrap_or(S3LOGS_TRANSFORM_DEFAULT_AGGREGATE_SECOND)),
            dedup_enabled: env::var("S3LOGS_TRANSFORM_LOG_DEDUPLICATION")
                    .map_or(S3LOGS_TRANSFORM_DEFAULT_LOG_DEDUPLICATION,
                            |x| x.parse::<bool>()
                    .unwrap_or(S3LOGS_TRANSFORM_DEFAULT_LOG_DEDUPLICATION)),
            cleanup_processed_logs: env::var("S3LOGS_TRANSFORM_CLEANUP_PROCESSED_LOGS")
                    .map_or(S3LOGS_TRANSFORM_DEFAULT_CLEANUP_PROCESSED_LOGS,
                            |x| x.parse::<bool>()
                    .unwrap_or(S3LOGS_TRANSFORM_DEFAULT_CLEANUP_PROCESSED_LOGS)),
            cleanup_uploaded_parquet: env::var("S3LOGS_TRANSFORM_CLEANUP_UPLOADED_PARQUET")
                    .map_or(S3LOGS_TRANSFORM_DEFAULT_CLEANUP_UPLOADED_PARQUET,
                            |x| x.parse::<bool>()
                    .unwrap_or(S3LOGS_TRANSFORM_DEFAULT_CLEANUP_UPLOADED_PARQUET)),
            file_buf_size: env::var("S3LOGS_FILE_BUF_SIZE")
                    .map_or(S3LOGS_FILE_DEFAULT_BUF_SIZE,
                            |x| x.parse::<usize>()
                    .unwrap_or(S3LOGS_FILE_DEFAULT_BUF_SIZE)),
            writer_bulk_lines: env::var("S3LOGS_TRANSFORM_PARQUET_WRTIER_BULK_LINES")
                    .map_or(S3LOGS_TRANSFORM_DEFAULT_PARQUET_WRTIER_BULK_LINES,
                            |x| x.parse::<usize>()
                    .unwrap_or(S3LOGS_TRANSFORM_DEFAULT_PARQUET_WRTIER_BULK_LINES)),
            writer_props: writer_props,
            line_parser: LineParser::new(),
            flock_timeout: Duration::from_secs(
                        env::var("S3LOGS_STAGGING_FILE_LOCK_TIMEOUT_SECONDS")
                            .map_or(S3LOGS_STAGGING_DEFAULT_FILE_LOCK_TIMEOUT_SECONDS,
                                    |x| x.parse::<u64>()
                            .unwrap_or(S3LOGS_STAGGING_DEFAULT_FILE_LOCK_TIMEOUT_SECONDS))
                        ),
            flock_retry_wait: Duration::from_millis(
                        env::var("S3LOGS_STAGGING_FILE_LOCK_RETRY_WAIT_MS")
                            .map_or(S3LOGS_STAGGING_DEFAULT_FILE_LOCK_RETRY_WAIT_MS,
                                    |x| x.parse::<u64>()
                            .unwrap_or(S3LOGS_STAGGING_DEFAULT_FILE_LOCK_RETRY_WAIT_MS))
                        ),
        }
    }

    pub fn self_copy(&self) -> Self {
        Self {
            bucket: self.bucket.clone(),
            region: self.region.clone(),
            target_prefix: self.target_prefix.clone(),
            tz: self.tz,
            stagging_root: self.stagging_root.clone(),
            stagging_dt_fmt: self.stagging_dt_fmt.clone(),
            stagging_suffix: self.stagging_suffix.clone(),
            parquet_root: self.parquet_root.clone(),
            prefix_fmt: self.prefix_fmt.clone(),
            schema: self.schema.clone(),
            job_interval: self.job_interval,
            aggregate_second: self.aggregate_second,
            dedup_enabled: self.dedup_enabled,
            cleanup_processed_logs: self.cleanup_processed_logs,
            cleanup_uploaded_parquet: self.cleanup_uploaded_parquet,
            file_buf_size: self.file_buf_size,
            writer_bulk_lines: self.writer_bulk_lines,
            writer_props: self.writer_props.clone(),
            line_parser: self.line_parser.clone(),
            flock_timeout: self.flock_timeout.clone(),
            flock_retry_wait: self.flock_retry_wait.clone(),
        }
    }

    pub async fn is_process_target(&self, path: &Path) -> bool {
        if let Ok(meta) = tokio::fs::metadata(path).await {
            if let Ok(modified) = meta.modified() {
                let now = SystemTime::now();
                let diff = now.duration_since(modified).unwrap_or_default().as_secs();
                let interval = self.job_interval;
                debug!("{:#?} timestamp backward {} - intenval {}", path.display(), diff, interval);
                if diff > interval {
                    return true;
                }
            }
        }
        false
    }

    pub async fn scan_stagging(&self) -> Result<Vec<String>, Error> {

        let root = Path::new(&self.stagging_root);
        let mut read = tokio::fs::read_dir(root).await?;

        let mut files = Vec::new();
        let mut processings = Vec::new();

        info!("start to scan stagging dir at {}", root.display());
        while let Some(entry) = read.next_entry().await? {

            let filename = entry.file_name().to_string_lossy().to_string();
            let s = StaggingFile::new_from_filename(&filename);
            if !s.is_valid() {
                // not a staggig file, skip
                continue;
            }

            if s.is_processing() {
                // someone is processing to parquet job, skip this
                processings.push(entry.file_name().to_string_lossy().to_string());
                continue;
            }

            if s.is_stagging() && self.is_process_target(&entry.path()).await {
                files.push(entry.file_name().to_string_lossy().to_string());
            }
        }

        // chance to detect orphan processing file
        for p in processings {

            info!("likely orpha file {}", p);
            let processing = StaggingFile::new_from_filename(&p).get_processing_fullpath();
            let res = tokio::fs::OpenOptions::new().read(true).write(true).open(&processing).await;
            if res.is_err() {
                // if open failed, don't care
                warn!("failed to open file {} - {:?}", processing, res.err().unwrap());
                continue;
            }

            let file = res.unwrap();
            // quick check with a single try lock
            match file.try_lock_exclusive() {
                Ok(_) => {},
                Err(_) => {
                    // someone is working on
                    info!("someone is working on, ignore {}", processing);
                    continue;
                }
            }

            let res = self.file_merge(&p).await;
            if res.is_err() {
                warn!("failed to merge back content for file {}", p);
                continue;
            }
            file.unlock()?;
            let filename = res.unwrap();
            let stagging = StaggingFile::new_from_filename(&filename);
            let fullpath = stagging.get_fullpath();
            let path = Path::new(&fullpath);
            if self.is_process_target(&path).await {
                files.push(filename);
            }
        }

        info!("scan stagging dir result: {:?}", files);
        Ok(files)
    }

    fn group_stagging_files(&self, input: Vec<String>) -> HashMap<(OrigBucket, TimeStamp), Vec<StaggingFile>> {

        debug!("start to group stagging files");
        let files: Vec<StaggingFile> = input.iter().map(|f| StaggingFile::new_from_filename(&f)).collect();
        debug!("got input stagging files: {:?}", files);

        // group by orig bucket
        let mut grouped: HashMap<String, Vec<StaggingFile>> = HashMap::new();
        for file in files {
            if let Some(v) = grouped.get_mut(&file.orig_bucket) {
                v.push(file);
            } else {
                let mut v = Vec::new();
                let orig_bucket = file.orig_bucket.to_string();
                v.push(file);
                grouped.insert(orig_bucket, v);
            }
        }
        debug!("grouped by orig bucket {:?}", grouped);

        let mut agg_group: HashMap<(OrigBucket, TimeStamp), Vec<StaggingFile>> = HashMap::new();
        for orig_bucket_group in grouped.values_mut() {
            // sort by datetime of filename in each group
            orig_bucket_group.sort_by(|a, b| a.datetime().partial_cmp(b.datetime()).unwrap());

            // split by time interval in this group
            for f in orig_bucket_group {
                let agg_ts = LineParser::timestamp_align_left(f.datetime_ts(), self.aggregate_second as usize);
                let orig_bucket = f.orig_bucket().to_string();
                let key = (orig_bucket, agg_ts);
                if let Some(v) = agg_group.get_mut(&key) {
                    v.push(f.self_copy());
                } else {
                    let mut v = Vec::new();
                    v.push(f.self_copy());
                    agg_group.insert(key, v);
                }
            }
        }
        agg_group
    }

    pub fn parse_stagging_file_datetime(&self, datetime: &str) -> Result<TimeStamp, Error> {

        let res = DateTime::parse_from_str(datetime, &self.stagging_dt_fmt);
        if res.is_err() {
            return Err(Error::new(ErrorKind::InvalidInput, "invalid datetime"));
        }

        Ok(res.unwrap().timestamp() as TimeStamp)
    }

    fn is_cross_bound(&self, last_datetime: TimeStamp, curr_datetime: TimeStamp) -> bool {

        let last = last_datetime/self.aggregate_second;
        let curr = curr_datetime/self.aggregate_second;

        if last < curr {
            return true;
        }

        assert!(last == curr);
        false
    }

    /*
     * @return - (max fields of output, number of lines in output)
     */
    fn parquet_writer_loop<W, R>(&self, schema: Arc<Schema>, mut writer: ArrowWriter<W>, mut lines: std::io::Lines<R>,
            startpoint: TimeStamp, max_fields: usize) -> Result<(usize, usize), Error>
    where
        W: std::io::Write,
        R: std::io::BufRead
    {

        let mut index = HashSet::new();
        let mut last_datetime = startpoint;
        let mut max: usize = max_fields;
        let mut need_extend: bool = false;
        let mut take = lines.by_ref().take(self.writer_bulk_lines);
        let mut total_lines = 0;
        loop {
            let v = take.filter_map(|l| {
                        let line = self.line_parser.extract_full(&l.unwrap(), false);
                        let _max = line.len();
                        if _max > max {
                            max = _max;
                            need_extend = true;
                        }
                        if !self.dedup_enabled {
                            return Some(line);
                        }

                        // do dedup
                        let request_id = line[5].clone();
                        let datetime = DateTime::parse_from_str(&line[2], S3_LOG_DATATIME_FMT)
                                            .ok().and_then(|dt| Some(dt.timestamp() as TimeStamp));

                        if datetime.is_none() {
                            error!("failed to parse Time field of log line, dump full line: {:?}", &line);
                        }
                        if self.is_cross_bound(last_datetime, datetime.unwrap()) {
                            index = HashSet::new();
                        }
                        last_datetime = datetime.unwrap();
                        if !index.insert(request_id) {
                            return None;
                        }
                        Some(line)
                    })
                    .collect::<Vec<Vec<String>>>();

            // if no more lines
            if v.len() == 0 {
                if need_extend {
                    info!("fields need to be extend to {}", max);
                    return Ok((max, total_lines));
                }
                debug!("no more log lines, jump to invoke flush on parquet writer");
                break;
            }

            // if found need to extend fields, we go through all lines
            if need_extend {
                take = lines.by_ref().take(self.writer_bulk_lines);
                continue;
            }

            let null = "".to_string();
            let arrays: ArrowResult<Vec<ArrayRef>> = (0..max)
                                                .map(|idx| {
                                                    Ok(Arc::new(v.iter()
                                                            .map(|row| row.get(idx).or(Some(&null)))
                                                            .collect::<StringArray>(),) as ArrayRef)
                                                })
                                                .collect();

            let batch = RecordBatch::try_new(Arc::clone(&schema), arrays.unwrap()).unwrap();
            writer.write(&batch)?;
            if log::log_enabled!(log::Level::Debug) {
                if let Some(usage) = memory_stats::memory_stats() {
                    debug!("{} rows wrote - memory phy: {}, virt: {}",
                        batch.num_rows(), usage.physical_mem, usage.virtual_mem);
                } else {
                    debug!("{} rows wrote - can not get memory usage", batch.num_rows());
                }
            }
            total_lines += batch.num_rows();
            take = lines.by_ref().take(self.writer_bulk_lines);
        }
        writer.flush()?;
        debug!("parquet data flushed");

        // writer must be closed to write footer
        writer.close()?;
        debug!("parquet file closed");

        Ok((max_fields, total_lines))
    }

    /*
     * @return - (locked input files, output parquet file, total lines in output)
     */
    pub fn write_to_parquet(&self, (orig_bucket, ts): (OrigBucket, TimeStamp), files: Vec<StaggingFile>)
            -> Result<(Vec<std::fs::File>, String, usize), Error> {

        let mut ofiles = Vec::new();

        for stagging_file in &files {
            let fullpath = stagging_file.get_fullpath();
            let processing = stagging_file.get_processing_fullpath();
            debug!("file path {} processing file path {}", fullpath, processing);

            if !Path::new(&fullpath).exists() {
                info!("source file does not exists {}", fullpath);
                return Err(Error::new(ErrorKind::Other, "source file not found"));
            }

            // test if processing is ongoing
            // this could happen when old logs are in stagging file,
            // as soon as we started processing, new coming logs could create new stagging file.
            if Path::new(&processing).exists() {
                info!("processing file exists {}", processing);
                return Err(Error::new(ErrorKind::Other, "processing file exists"));
            }

            debug!("opening file {}", &fullpath);
            let ifile = std::fs::File::options().read(true).write(true).open(&fullpath)?;

            debug!("lock file {}", &fullpath);
            lock_exclusive_try(&ifile, self.flock_timeout, self.flock_retry_wait)?;
            let meta = ifile.metadata()?;
            if meta.len() == 0 {
                warn!("file size is zero, skip this one");
                //return Err(Error::new(ErrorKind::Other, "file size is 0, skip this one"));
            }
            ofiles.push(ifile);
        }

        // we locked all stagging files, we can safely rename them now

        for stagging_file in &files {
            let fullpath = stagging_file.get_fullpath();
            let processing = stagging_file.get_processing_fullpath();
            debug!("rename file from {} to {}", &fullpath, &processing);
            std::fs::rename(&fullpath, &processing)
                                .map_err(|_| {
                                    warn!("failed to rename {} to {}",  fullpath, processing);
                                    Error::new(ErrorKind::Other, "rename failed")
                                })?;
        }
        debug!(" all files renamed");

        let vec_files: Vec<String> = files.iter().map(|f| f.get_processing_fullpath()).collect();
        debug!("concat processing files {:?}", &vec_files);
        let concat = concat_path(&vec_files);
        let lines = std::io::BufReader::with_capacity(self.file_buf_size, concat).lines();

        let parquet_filepath = self.gen_parquet_filepath(StaggingFile::new_from_ts(&orig_bucket, self.tz, ts));
        let incomplete_parquet_filepath = format!("{}.incomplete", parquet_filepath);
        let ofile = std::fs::File::create(&incomplete_parquet_filepath).unwrap();

        let writer = ArrowWriter::try_new(&ofile, Arc::new(self.schema.clone()), Some(self.writer_props.clone())).unwrap();

        info!("start to read input file");
        let mut stat = TimeStats::new();

        let mut total_lines;
        let max_fields = self.schema.fields().len();
        let (actual_max_fields, lines_done) = self.parquet_writer_loop(Arc::new(self.schema.clone()), writer, lines, ts, max_fields)?;
        total_lines = lines_done;
        if actual_max_fields > max_fields {

            // we need to extend schema, let's start over again with new max fields
            info!("extra {} fields needed, fail through to extend", actual_max_fields - max_fields);
            let extra = (max_fields+1..actual_max_fields+1).map(|x| {
                                    let extra_name = format!("ExtraField{}", x);
                                    Field::new(&extra_name, DataType::Utf8, true)
                                }).collect();
            let new_schema = Schema::try_merge(vec![self.schema.clone(), Schema::new(extra)])
                                        .map_err(|_| Error::new(ErrorKind::InvalidInput, "failed to merege schema"))?;

            debug!("schema extended to {} fields", new_schema.fields().len());
            debug!("recreate output parquet file");
            let ofile = std::fs::File::create(&incomplete_parquet_filepath)?;

            let writer = ArrowWriter::try_new(&ofile, Arc::new(new_schema.clone()), Some(self.writer_props.clone())).unwrap();

            let concat2 = concat_path(vec_files);
            let lines = std::io::BufReader::with_capacity(self.file_buf_size, concat2).lines();
            let (_max, lines_done) = self.parquet_writer_loop(Arc::new(new_schema.clone()), writer, lines, ts, actual_max_fields)?;
            total_lines = lines_done;
            assert!(actual_max_fields == _max);
        }

        std::fs::rename(&incomplete_parquet_filepath, &parquet_filepath)
            .map_err(|e| {
                warn!("failed to rename {} to {}", incomplete_parquet_filepath, parquet_filepath);
                e
            })?;
        info!("output parquet file at {}, cost: {}", parquet_filepath, stat.elapsed());
        Ok((ofiles, parquet_filepath, total_lines))
    }

    pub fn transform_cleanup(&self, locked_files: Vec<std::fs::File>, files: Vec<StaggingFile>) -> Result<(), Error> {

        info!("start cleanup process");
        let mut prearchives = Vec::new();
        for stagging_file in &files {
            let fullpath = stagging_file.get_processing_fullpath();
            if !Path::new(&fullpath).exists() {
                warn!("log file {} not found", &fullpath);
                return Err(Error::new(ErrorKind::NotFound, "log file not found"));
            }

        }

        // TODO: atomic rename all processing to archive
        for stagging_file in files {
            let fullpath = stagging_file.get_processing_fullpath();
            let prearchive = stagging_file.get_archive_fullpath();
            info!("rename file from {} to {}", fullpath, prearchive);
            std::fs::rename(&fullpath, &prearchive)?;
            prearchives.push(prearchive);
        }

        // release file lock
        for flock in locked_files {
            flock.unlock()?;
        }

        for prearchive in prearchives {

            if self.cleanup_processed_logs {
                info!("remove prearchive file {}", prearchive);
                std::fs::remove_file(&prearchive)?;
                continue;
            }

            let gzfile = format!("{}.gz", prearchive);
            let gz = std::fs::File::create(&gzfile)?;
            info!("gzip file to {}", gzfile);
            let mut gz = GzBuilder::new()
                    .comment("s3 logs archive file")
                    .write(gz, flate2::Compression::best());
            let orig = std::fs::File::open(&prearchive)?;
            let mut reader = std::io::BufReader::with_capacity(self.file_buf_size, orig);
            std::io::copy(&mut reader, &mut gz)?;
            gz.finish()?;

            info!("gzip done, remove archive file {}", prearchive);
            std::fs::remove_file(&prearchive)?;
        }
        Ok(())
    }

    async fn file_merge(&self, processing_filename: &str) -> Result<String, Error> {

        let processing = format!("{}/{}", self.stagging_root, processing_filename);

        let re = regex::Regex::new(r#"(\S+).processing"#).unwrap();
        let caps = re.captures(processing_filename).unwrap();

        let stagging_filename = caps.get(1).unwrap().as_str();
        let stagging = format!("{}/{}", self.stagging_root, stagging_filename);

        info!("merge {} back to {}", processing, stagging);
        let file = tokio::fs::OpenOptions::new()
                            .write(true)
                            .create(true)
                            .append(true)
                            .open(&stagging).await?;
        if true {
            backoff_lock_try(&file, &stagging, 10, 25, self.flock_timeout).await?;
        } else {
            let flock = FileLock::new(&file, self.flock_timeout);
            flock.await?;
        }
        debug!("locked file {}", &stagging);

        let rd = tokio::fs::File::open(&processing).await?;

        let mtime;
        // we need to preserve atime/mtime depends on 2 file's sitruation
        let s_meta = tokio::fs::metadata(&stagging).await?;
        if s_meta.size() > 0 {
            // if stagging file exists, preserve from its
            mtime = s_meta.mtime();
        } else {
            // preserve processing's
            let p_meta = tokio::fs::metadata(&processing).await?;
            mtime = p_meta.mtime();
        }

        let mut reader = tokio::io::BufReader::with_capacity(self.file_buf_size, rd);
        let mut writer = tokio::io::BufWriter::with_capacity(self.file_buf_size, file);
        tokio::io::copy(&mut reader, &mut writer).await?;
        debug!("content merged");
        writer.flush().await?;

        tokio::fs::remove_file(&processing).await.unwrap_or_default();
        info!("removed processing file {}", processing);

        if let Err(e) = set_mtime(&stagging, mtime) {
            // in case preserve time failed, we just raise a warning
            warn!("failed to change mtime {}, err: {}", &stagging, e);
        }
        // lock will automatic release here
        //file.unlock()?;
        debug!("unlocked file {}", &stagging);
        Ok(stagging_filename.to_string())
    }

    pub fn transform_recovery(&self, files: Vec<StaggingFile>, parquet_filepath: Option<&str>) -> Result<(), Error> {

        info!("recovery process started");

        // cleanup parquet file generated if exists
        if parquet_filepath.is_some() {
            std::fs::remove_file(parquet_filepath.unwrap()).unwrap_or_default();
            info!("removed parquet file {}", parquet_filepath.unwrap());
        }

        // failthrough

        for stagging_file in files {
            let processing = stagging_file.get_processing_fullpath();
            let stagging = stagging_file.get_fullpath();
            info!("merge {} back to {}", processing, stagging);
            let file = std::fs::OpenOptions::new()
                                .write(true)
                                .create(true)
                                .append(true)
                                .open(&stagging)?;
            lock_exclusive_try(&file, self.flock_timeout, self.flock_retry_wait)?;
            debug!("locked file {}", &stagging);

            let rd = std::fs::File::open(&processing)?;
            let mut reader = std::io::BufReader::with_capacity(self.file_buf_size, rd);
            let mut writer = std::io::BufWriter::with_capacity(self.file_buf_size, &file);
            std::io::copy(&mut reader, &mut writer)?;
            debug!("content merged");
            writer.flush()?;

            file.unlock()?;
            debug!("unlocked file {}", &stagging);

            std::fs::remove_file(&processing).unwrap_or_default();
            info!("removed processing file {}", processing);
        }

        Ok(())
    }

    fn get_s3_prefix_partition_part(&self, input: &str) -> String {
        let dt = DateTime::parse_from_str(input, &self.stagging_dt_fmt);
        dt.unwrap_or_default().format(&self.prefix_fmt).to_string()
    }

    fn get_s3_parquet_key(&self, input: &str, parquet_filepath: &str) -> String {
        let mut s: Vec<&str> = parquet_filepath.split("/").collect();
        let parquet_filename = s.pop().unwrap_or_default();
        if self.target_prefix == "" {
            return format!("{}/{}", self.get_s3_prefix_partition_part(input), parquet_filename);
        } else {
            return format!("{}/{}/{}", self.target_prefix, self.get_s3_prefix_partition_part(input), parquet_filename);
        }
    }

    fn gen_parquet_filepath(&self, stagging_file: StaggingFile) -> String {
        return format!("{}/{}_{}.{}.parquet", self.parquet_root,
                    stagging_file.orig_bucket(), stagging_file.datetime(),
                    Alphanumeric.sample_string(&mut rand::thread_rng(), 16));
    }

    /*
     * @return - total lines in output
     */
    pub async fn transform_parquet(&self, (orig_bucket, ts): (OrigBucket, TimeStamp), files: Vec<StaggingFile>) -> Result<usize, Error> {

        info!("start transform {:?} from orig bucket {} at timestamp {} to parquet at thread {:?}",
            files.iter().map(|f| f.get_fullpath()).collect::<Vec<String>>(), orig_bucket, ts, std::thread::current().id());
        let copy_files: Vec<StaggingFile> = files.iter().map(|f| f.self_copy()).collect();
        let me = Self::self_copy(self);
        let orig_bucket_clone = orig_bucket.clone();
        let res = tokio::task::spawn_blocking(move || {
            me.write_to_parquet((orig_bucket_clone, ts), copy_files)
        }).await?;
        match res {
            Ok((flocks, parquet_filepath, total_lines)) => {
                let tm = TransferManager::new(&self.region).await;
                let tmp_file = StaggingFile::new_from_ts(&orig_bucket, self.tz, ts);
                let s3_parquet_key = self.get_s3_parquet_key(tmp_file.datetime(), &parquet_filepath);
                info!("initializing upload local parquet file to {}", s3_parquet_key);
                let res = tm.upload_object(&parquet_filepath, &self.bucket, &s3_parquet_key, 0).await;
                if res.is_ok() {
                    if self.cleanup_uploaded_parquet {
                        match tokio::fs::remove_file(&parquet_filepath).await {
                            Ok(_) => {
                                info!("removed parquet file {}", parquet_filepath);
                            },
                            Err(e) => {
                                warn!("failed to remove parquet file {}, err: {:?}", parquet_filepath, e);
                            }
                        }
                    }
                    self.transform_cleanup(flocks, files)?;
                    return Ok(total_lines);
                } else {
                    debug!("error occur: {:?}", res.err());
                    self.transform_recovery(files, Some(&parquet_filepath))?;
                    return Ok(0);
                }
            },
            Err(err) => {
                match err.kind() {
                    ErrorKind::Other => {
                    },
                    _ => {
                        warn!("error occur: {:?}", err);
                        self.transform_recovery(files, None)?;
                        return Ok(0);
                    },
                }
            },
        }

        Ok(0)
    }

    pub async fn process_stagging_dir(&self, threads: Option<usize>) -> Result<usize, Error> {

        let files = self.scan_stagging().await?;

        let grouped: HashMap<(OrigBucket, TimeStamp), Vec<StaggingFile>> = self.group_stagging_files(files);

        let mut joins = Vec::new();

        for ((orig_bucket, ts), files) in grouped {

            let me = Self::self_copy(self);
            info!("initialized transform process for {:?} from orig bucket {} at timestamp {}",
                files.iter().map(|f| f.get_fullpath()).collect::<Vec<String>>(), orig_bucket, ts);
            let join = tokio::spawn(async move {
                me.transform_parquet((orig_bucket, ts), files).await
            });
            joins.push(join);
            if threads.is_some() {
                if joins.len() == threads.unwrap() {
                    info!("spawned {} threads", threads.unwrap());
                    break;
                }
            }
        }

        let mut total_lines = 0;
        // ignore any error in parquet transform process
        for join in joins {
            if let Ok(lines) = join.await? {
                total_lines += lines;
            }
        }

        Ok(total_lines)
    }

    pub async fn process_single_file(&self, filename: &str) -> Result<usize, Error> {

        let stagging_file = StaggingFile::new_from_filename(filename);

        if !stagging_file.is_stagging() {
            return Err(Error::new(ErrorKind::InvalidInput, "invalid filename"));
        }

        let total_lines = self.transform_parquet((stagging_file.orig_bucket.clone(), stagging_file.datetime_ts()), vec![stagging_file]).await?;

        Ok(total_lines)
    }
}
