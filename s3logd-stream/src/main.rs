mod output;
mod conf;
mod transfer;
pub(crate) mod mon;
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::VecDeque;
use futures::future::join_all;
use tokio::sync::RwLock;
use tokio::io::{Error, ErrorKind};
use tokio::time::{sleep, Duration};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_sqs::Client;
use aws_sdk_sqs::config::Region;
use aws_sdk_sqs::types::Message;
use aws_lambda_events::s3::{S3Event, S3EventRecord};
use config::Config;
use structopt::StructOpt;
use daemonize::Daemonize;
use log::{info, warn, trace};

const DEFAULT_LOG_LEVEL: &str = concat!(env!("CARGO_PKG_NAME"), "=info,s3logs=info");
const DEFAULT_LOG_FILE: &str = concat!(env!("CARGO_PKG_NAME"), ".log");
const DEFAULT_LOG_ROTATE_SIZE: u64 = 50; // in MB
const DEFAULT_LOG_KEEP_FILES: u64 = 100;
const DEFAULT_LOG_DIRECTORY: &str = "logs";
const DEFAULT_NUM_WORKERS: u64 = 2;
const DEFAULT_MAX_SQS_MESSAGES: i32 = 10;
const DEFAULT_WAIT_TIME_SECONDS: i32 = 20;
const DEFAULT_RECV_IDLE_SECONDS: u64 = 15;
const DEFAULT_NUM_EXECUTORS: u64 = 1;

struct TaskQueue {
    inner: RwLock<VecDeque<Message>>,
}

impl TaskQueue {
    fn new() -> Self {
        Self {
            inner: RwLock::new(VecDeque::new()),
        }
    }

    async fn push(&self, msg: Message) {
        let mut v = self.inner.write().await;
        v.push_back(msg);
    }

    async fn pop(&self) -> Option<Message> {
        let mut v = self.inner.write().await;
        v.pop_front()
    }

    async fn len(&self) -> usize {
        let v = self.inner.read().await;
        v.len()
    }
}

struct Executor {
    client: Arc<Client>,
    recv_queue: String,
    recv_max_msgs: i32,
    recv_pollwait_sec: i32,
    recv_idle_sec: u64,
    recv_queue_len: i32,
    workers: u64,
    queue: Arc<TaskQueue>,
    mgr: output::Manager,
}

impl Executor {

    async fn new(region: &str, queue: &str,
            recv_max_msgs: i32, recv_pollwait_sec: i32,
            recv_idle_sec: u64, recv_queue_len: i32, workers: u64, mgr: output::Manager) -> Self {

        let region_provider = RegionProviderChain::first_try(Region::new(region.to_owned()))
            .or_default_provider()
            .or_else(Region::new("us-west-2"));

        let shared_config = aws_config::defaults(aws_config::BehaviorVersion::latest()).region(region_provider).load().await;

        Self {
            client: Arc::new(Client::new(&shared_config)),
            recv_queue: queue.to_string(),
            recv_max_msgs: recv_max_msgs,
            recv_pollwait_sec: recv_pollwait_sec,
            recv_idle_sec: recv_idle_sec,
            recv_queue_len: recv_queue_len,
            workers: workers,
            queue: Arc::new(TaskQueue::new()),
            mgr: mgr,
        }
    }

    fn copy_me(&self) -> Self {
        Self {
            client: self.client.clone(),
            recv_queue: self.recv_queue.clone(),
            recv_max_msgs: self.recv_max_msgs,
            recv_pollwait_sec: self.recv_pollwait_sec,
            recv_idle_sec: self.recv_idle_sec,
            recv_queue_len: self.recv_queue_len,
            workers: self.workers,
            queue: self.queue.clone(),
            mgr: self.mgr.clone(),
        }
    }

    async fn poll_msgs(&self) -> Result<Vec<Message>, Error> {

        let res = self.client
                        .receive_message()
                        .max_number_of_messages(self.recv_max_msgs)
                        .wait_time_seconds(self.recv_pollwait_sec)
                        .queue_url(&self.recv_queue)
                        .send()
                        .await;
        match res {
            Ok(_) => {
            },
            Err(err) => match err {
                aws_sdk_sqs::error::SdkError::ServiceError(err) => {
                    warn!("sdk error: {:?}", err);
                    return Err(Error::new(ErrorKind::Other, "failed to receive messages from queue"));
                },
                _ => {
                    return Err(Error::new(ErrorKind::Other, "failed to receive messages from queue"));
                },
            },
        }

        let output = res.unwrap();
        let msgs: Vec<Message> = output.messages().to_vec();

        return Ok(msgs);
    }

    async fn retrieve_msgs_loop(&self, quit: Arc<AtomicBool>) -> Result<(), Error> {

        info!("retrieve messages loop started");

        // infinite loop retrieve all sqs messages
        loop {

            let res = self.poll_msgs().await;
            if res.is_err() {
                warn!("polling error: {:?}", res.unwrap());
                continue;
            }
            let msgs = res.unwrap();
            trace!("polling get msgs: {}", msgs.len());

            if quit.load(Ordering::SeqCst) {
                info!("catch quit signal, wakeup all worker to quit ...");
                info!("retrieve message loop quit ...");
                break;
            }

            for msg in msgs {
                self.queue.push(msg).await;
            }

            loop {
                let qlen = self.queue.len().await;
                if qlen >= (self.recv_queue_len - self.recv_max_msgs) as usize {
                    // queue almost full, sleep for a while
                    trace!("recv queue {}/{} sleep {} seconds", qlen, self.recv_queue_len, self.recv_idle_sec);
                    sleep(Duration::from_secs(self.recv_idle_sec)).await;
                    if quit.load(Ordering::SeqCst) {
                        break;
                    }
                } else {
                    // poll new messages to fill up queue
                    break;
                }
            }
        }

        Ok(())
    }

    async fn spawn_workers(&self, quit: Arc<AtomicBool>) -> Vec<tokio::task::JoinHandle<()>> {

        let mut tasks = Vec::new();
        for worker in 0..self.workers {
            let me = self.copy_me();
            let _quit = quit.clone();
            let wrk = tokio::task::spawn(async move {
                info!("worker #{} started", worker);
                loop {
                    if _quit.load(Ordering::SeqCst) {
                        info!("quit signal received in worker {:?}", std::thread::current().id());
                        break;
                    }
                    let msg = me.queue.pop().await;
                    if msg.is_none() {
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    // ignore any error and continue
                    let _ = me.handle_one_msg(msg.unwrap()).await;
                }
            });
            tasks.push(wrk);
        }
        tasks
    }

    #[allow(dead_code)]
    async fn del_msg(&self, receipt: &str) -> Result<(), Error> {

        let res = self.client
                        .delete_message()
                        .queue_url(&self.recv_queue)
                        .receipt_handle(receipt)
                        .send()
                        .await;
        match res {
            Ok(_) => {
                return Ok(())
            },
            Err(err) => match err {
                aws_sdk_sqs::error::SdkError::ServiceError(err) => {
                    warn!("sdk error: {:?}", err)
                },
                _ => {
                },
            },
        }
        return Err(Error::new(ErrorKind::Other, "failed to delete message from queue"));
    }

    async fn s3_event_handler(&self, record: S3EventRecord, receipt: String) -> Result<(), Error> {

        let region = record.aws_region.unwrap_or_default();
        let bucket = record.s3.bucket.name.unwrap_or_default();
        let key = record.s3.object.key.unwrap_or_default();
        let size = record.s3.object.size.unwrap_or_default();
        let event_name = record.event_name.unwrap_or_default();

        if event_name != "ObjectCreated:CompleteMultipartUpload"
            && event_name != "ObjectCreated:Put" {

            warn!("skip non PUT event: {} for region: {}, bucket: {}, key: {}, size: {}",
                    event_name, region, bucket, key, size);
            return Ok(())
        }

        trace!("start log aggregation task for region: {}, bucket: {}, key: {}, size: {}",
                    region, bucket, key, size);

        let res = self.mgr.process_s3(&self.recv_queue, self.client.clone(), &region, &bucket, &key, &receipt).await;
        if res.is_err() {
            return Err(Error::new(ErrorKind::Other, "process failed"));
        }

        Ok(())
    }

    async fn handle_one_msg(&self, msg: Message) -> Result<(), Error> {

        let body = if msg.body.is_some() {
            msg.body.as_ref().unwrap()
        } else {
            panic!("invalid SQS message body {:?}", msg);
        };
        let receipt: String = if msg.receipt_handle.is_some() {
            msg.receipt_handle.as_ref().unwrap().to_string()
        } else {
            panic!("invalid SQS message receipt handle {:?}", msg);
        };

        if let Ok(s3event) = serde_json::from_str::<S3Event>(body) {
            if s3event.records.len() > 1 {
                warn!("too many S3 event records in one SQS msg, is it correct?");
            } else {
                if let Ok(_) = self.s3_event_handler(s3event.records[0].clone(), receipt.clone()).await {
                    // TODO
                    //let _ = self.del_msg(&receipt).await;
                    trace!("sqs receipt {} finished", receipt);
                }
            }
        }

        Ok(())
    }

    async fn entry(&self, quit: Arc<AtomicBool>) {

        let mut tasks = Vec::new();

        let _quit = quit.clone();
        let me = self.copy_me();
        let mtask = tokio::task::spawn(async move {
            let _ = me.retrieve_msgs_loop(_quit).await;
        });
        tasks.push(mtask);

        let mut workers = self.spawn_workers(quit.clone()).await;

        tasks.append(&mut workers);
        join_all(tasks).await;
    }
}

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(short, long, display_order=1, help="input config.ini")]
    config: String,
    #[structopt(short, long, display_order=2, help="running in daemon")]
    daemon: bool,
}

fn main() {

    let Opt {
        config,
        daemon,
    } = Opt::from_args();

    let table = Config::builder()
            .add_source(config::File::with_name(&config))
            .build()
            .expect("unable to open config file")
            .get_table("STREAM")
            .expect("unable to get STREAM section");
    let region = table.get("region")
                        .expect("unable to get region from config")
                        .to_owned()
                        .into_string()
                        .expect("incorrect region field in config");
    let queue = table.get("queue")
                        .expect("unable to get queue from config")
                        .to_owned()
                        .into_string()
                        .expect("incorrect queue field in config");

    let config_loglevel = table.get("loglevel")
                        .unwrap_or(&config::Value::from(DEFAULT_LOG_LEVEL))
                        .to_owned()
                        .into_string()
                        .unwrap();
    let logfile = table.get("logfile")
                        .unwrap_or(&config::Value::from(DEFAULT_LOG_FILE))
                        .to_owned()
                        .into_string()
                        .unwrap();
    let log_rotate_size = table.get("log_rotate_size")
                        .unwrap_or(&config::Value::from(DEFAULT_LOG_ROTATE_SIZE))
                        .to_owned()
                        .into_uint()
                        .unwrap();
    let log_keep_files = table.get("log_keep_files")
                        .unwrap_or(&config::Value::from(DEFAULT_LOG_KEEP_FILES))
                        .to_owned()
                        .into_uint()
                        .unwrap();
    let log_directory = table.get("log_directory")
                        .unwrap_or(&config::Value::from(DEFAULT_LOG_DIRECTORY))
                        .to_owned()
                        .into_string()
                        .unwrap();
    let workers = table.get("num_workers")
                        .unwrap_or(&config::Value::from(DEFAULT_NUM_WORKERS))
                        .to_owned()
                        .into_uint()
                        .unwrap();
    let executors = table.get("num_executors")
                        .unwrap_or(&config::Value::from(DEFAULT_NUM_EXECUTORS))
                        .to_owned()
                        .into_uint()
                        .unwrap();
    let recv_max_msgs = table.get("max_sqs_messages")
                        .unwrap_or(&config::Value::from(DEFAULT_MAX_SQS_MESSAGES))
                        .to_owned()
                        .try_deserialize()
                        .unwrap();
    let recv_pollwait_sec = table.get("sqs_wait_time_seconds")
                        .unwrap_or(&config::Value::from(DEFAULT_WAIT_TIME_SECONDS))
                        .to_owned()
                        .try_deserialize()
                        .unwrap();
    let recv_idle_sec = table.get("sqs_poll_idle_seconds")
                        .unwrap_or(&config::Value::from(DEFAULT_RECV_IDLE_SECONDS))
                        .to_owned()
                        .into_uint()
                        .unwrap();
    let mut recv_queue_len = table.get("max_recv_queue_len")
                        .unwrap_or(&config::Value::from(DEFAULT_RECV_IDLE_SECONDS))
                        .to_owned()
                        .try_deserialize()
                        .unwrap();
    // reset queue len if it is too small
    if recv_queue_len < recv_max_msgs {
        recv_queue_len = recv_max_msgs;
    }

    let output_config = output::OutputConfig::new(&config);

    let loglevel = std::env::var("RUST_LOG").unwrap_or(config_loglevel.to_string());

    let quit = Arc::new(AtomicBool::new(false));

    let logger_handler = if daemon {
        let daemonize = Daemonize::new()
                    .pid_file("s3logd.pid")
                    .working_directory("./");

        if let Err(e) = daemonize.start() {
            eprintln!("error occur when starting: {}", e);
            process::exit(1);
        }

        // log output style
        fn env_format(
            w: &mut dyn std::io::Write,
            now: &mut flexi_logger::DeferredNow,
            record: &log::Record,
        ) -> Result<(), std::io::Error> {
            write!(
                w,
                "[{} {} {}] {}",
                now.format_rfc3339(),
                record.level(),
                record.module_path().unwrap_or("<unnamed>"),
                &record.args()
            )
        }

        let flexi_logger = flexi_logger::Logger::try_with_str(&loglevel).unwrap();

        let logfilespec = flexi_logger::FileSpec::default()
            .directory(log_directory)
            .basename(env!("CARGO_PKG_NAME"))
            .suffix("log");
        let lh = flexi_logger.log_to_file(logfilespec)
            .create_symlink(&logfile)
            .write_mode(flexi_logger::WriteMode::BufferAndFlush)
            .format(env_format)
            .rotate(
                flexi_logger::Criterion::Size(log_rotate_size*1024*1024),
                flexi_logger::Naming::Numbers,
                flexi_logger::Cleanup::KeepCompressedFiles(log_keep_files.try_into().unwrap())
            )
            .start()
            .unwrap();
        Some(lh)
    } else {
        let mut builder = env_logger::Builder::new();
        builder.parse_filters(&loglevel);
        builder.init();
        None
    };

    if daemon {
        info!("started as daemon");
        info!("logfile: {}", logfile);
        info!("log_rotate_size: {} MB", log_rotate_size);
        info!("log_keep_files: {}", log_keep_files);
    } else {
        info!("started as foreground");
    }
    info!("loglevel: {}", loglevel);
    info!("queue: {}", queue);
    info!("num_workers: {}", workers);
    info!("max_sqs_messages: {}", recv_max_msgs);
    info!("sqs_wait_time_seconds: {}", recv_pollwait_sec);
    info!("sqs_poll_idle_seconds: {}", recv_idle_sec);
    info!("max_recv_queue_len: {}", recv_queue_len);

    let q = quit.clone();
    ctrlc::set_handler(move || {
        q.store(true, Ordering::SeqCst);
        warn!("quit signal received");
    }).expect("setting signal handler failed");

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .max_blocking_threads(workers as usize * 2)
        .build()
        .unwrap()
        .block_on(async {

            let mut set = tokio::task::JoinSet::new();

            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let q = quit.clone();
            set.spawn(async move {
                mon::mon_task(q, rx).await;
            });

            for _ in 0..executors {
                let quit = quit.clone();
                let region = region.to_string();
                let queue = queue.to_string();
                let oc = output_config.clone();
                let monchan = tx.clone();
                let be_quit = Arc::new(AtomicBool::new(false));

                set.spawn(async move {
                    let mgr = output::Manager::new(be_quit.clone(), monchan, oc);
                    let exec = Executor::new(&region, &queue,
                        recv_max_msgs, recv_pollwait_sec,
                        recv_idle_sec, recv_queue_len, workers, mgr).await;
                    exec.entry(quit.clone()).await;
                    exec.mgr.shutdown().await;
                });
            }

            while let Some(res) = set.join_next().await {
                let _ = res.unwrap();
            }
        });
    info!("all tasks have quit, exit program...");
    if let Some(handler) = logger_handler {
        handler.flush();
    }
}
