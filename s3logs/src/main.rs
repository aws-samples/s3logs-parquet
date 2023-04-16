use std::path::PathBuf;
use env_logger;
use s3logs::utils::{S3LogAggregator, S3LogTransform};
use tokio::io::Error;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
enum AggregateSubCmd {
    #[structopt(about = "process S3 server access log from S3")]
    S3 {
        #[structopt(short, display_order = 1, help = "region")]
        region: String,
        #[structopt(short, display_order = 2, help = "bucket")]
        bucket: String,
        #[structopt(short, display_order = 3, help = "key")]
        key: String,
        #[structopt(short, display_order = 4, help = "max threads to use, if not supplied, set to num of CPUs")]
        threads: Option<usize>,
    },
    #[structopt(about = "process S3 server access log from local filesystem")]
    Local {
        #[structopt(parse(from_os_str), display_order = 1, help = "logs from local filesystem")]
        input: Vec<PathBuf>,
        #[structopt(short, display_order = 2, help = "max threads to use, if not supplied, set to num of CPUs")]
        threads: Option<usize>,
    },
}

#[derive(Debug, StructOpt)]
enum SubCmd {
    #[structopt(display_order = 1)]
    Aggregate {
        #[structopt(subcommand)]
        cmd: AggregateSubCmd,
    },
    #[structopt(display_order = 2)]
    Transform {
        #[structopt(short, display_order = 1, help = "output parquet region")]
        region: String,
        #[structopt(short, display_order = 2, help = "output parquet bucket")]
        bucket: String,
        #[structopt(parse(from_os_str), display_order = 3,
            help = "input log files, if NOT supplied, scan S3LOGS_STAGGING_ROOT_PATH")]
        input: Option<PathBuf>,
        #[structopt(short, display_order = 4, help = "max threads to use, if not supplied, set to num of CPUs")]
        threads: Option<usize>,
    },
}

async fn cli_main(opt: SubCmd) -> Result<(), Error> {

    match opt {
        SubCmd::Aggregate { cmd } => {
            match cmd {
                AggregateSubCmd::S3 {region, bucket, key, threads: _} => {
                    let parser = S3LogAggregator::new(&region, &bucket, &key, None, None, None, None);
                    let total = parser.process_s3().await?;
                    println!("{} of lines processed", total);
                },
                AggregateSubCmd::Local {input, threads: _} => {
                    if input.len() == 1 && input.get(0).unwrap().is_dir() {
                        let root = input.get(0).unwrap().as_path();
                        let mut reads = tokio::fs::read_dir(root).await?;
                        while let Some(entry) = reads.next_entry().await? {
                            if let Ok(metadata) = entry.metadata().await {
                                if !metadata.is_file() {
                                    continue;
                                }
                                let parser = S3LogAggregator::new("", "", "", None, None, None, None);
                                if let Ok(total) = parser.process_local(&entry.path().to_string_lossy()).await {
                                    println!("{} of lines processed for input {:?}", total, entry);
                                } else {
                                    println!("process failed input {:?}", entry);
                                }
                            }
                        }
                        return Ok(());
                    }

                    for file in input {
                        if file.is_file() {
                            let parser = S3LogAggregator::new("", "", "", None, None, None, None);
                            if let Ok(total) = parser.process_local(file.as_path().to_str().unwrap()).await {
                                println!("{} of lines processed for input {}", total, file.display());
                            } else {
                                println!("process failed input {}", file.display());
                            }
                        }
                    }
                }
            }
        },
        SubCmd::Transform {region, bucket, input, threads: _} => {

            let trans = S3LogTransform::new(&region, &bucket, None, None, None);

            if input.is_none() {
                let _ = trans.process_stagging_dir().await;
                return Ok(());
            }

            let pathbuf = input.unwrap();
            if pathbuf.is_file() {
                trans.process_single_file(pathbuf.file_name().unwrap().to_str().unwrap()).await?;
            } else {
                panic!("invalid input");
            }
        },
    }

    Ok(())
}

fn main() -> Result<(), Error> {

    env_logger::init_from_env(
            env_logger::Env::default()
                        .filter_or(env_logger::DEFAULT_FILTER_ENV, format!("{}=info", env!("CARGO_PKG_NAME")))
    );

    let opt = SubCmd::from_args();

    let opt_threads;
    match &opt {
        SubCmd::Aggregate { cmd } => {
            match cmd {
                AggregateSubCmd::S3 {region: _, bucket: _, key: _, threads} => {
                    opt_threads = threads;
                },
                AggregateSubCmd::Local {input: _, threads} => {
                    opt_threads = threads;
                }
            }
        },
        SubCmd::Transform {region: _, bucket: _, input: _, threads} => {
            opt_threads = threads;
        },
    }

    let mut rt_builder = tokio::runtime::Builder::new_multi_thread();

    if opt_threads.is_none() {
        rt_builder.enable_all();
    } else {
        rt_builder.enable_all()
            .worker_threads(opt_threads.unwrap());
    }

    rt_builder.build()
        .unwrap()
        .block_on(async {
            cli_main(opt).await
        })
}
