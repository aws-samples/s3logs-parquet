# A Rust implementation for AWS S3 server access log Extract and Transform
Tools to transform AWS S3 server access log to parquet format by various user-defined aggregation parameters and upload back to S3 by partitioned prefix.

## Features
- Aggregate with customer defined datetime granularity
- Aggregate by orig bucket
- Aggregate by customer defined timezone
- Transform to parquet for Hadoop friendly
- Reduce total size by compression in parquet format
- Adoptive log fields extension (for further log fields expand)
- Partitioned parquet upload to S3 with customer defined prefix format

## S3 Server Access Log Format
For more information about S3 server access log format, please visit:
https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html

## Supported platforms
- x86_64
- aarch64

## Modules
[```s3logs```](s3logs) Implementation of core S3 logs aggregate and transform logic together with simple cli, see [Use CLI](s3logs/README.md#use-cli) for more details.

[```s3logd```](s3logd) S3 logs aggregator daemon, could be running on EC2 standalone.

[```s3log-lambda-aggregator```](s3log-lambda-aggregator) Lambda implementation of S3 log aggregator.

[```s3log-lambda-transformer```](s3log-lambda-transformer) Lambda implementation of S3 log transformer.

## How to build
### Install Rust
```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```
in case this is the first time you on board rust, please install:
```
sudo yum install gcc
```
### Build binary
inside of project folder, run:
```
cargo build --release
```
you will find binary at `target/release`

### Environment settings
| Environment | Description | Default |
| ----------- | ----------- | ------- |
| S3LOGS_STAGGING_ROOT_PATH | location of aggregation(stagging) files store | /mnt/s3logs/stagging |
| S3LOGS_STAGGING_PARTITION_SECOND | time granularity at which log is partitioned (15 Min) | 900 |
| S3LOGS_STAGGING_PARTITION_TZIF | time zone of your partitioned logs aligned to | UTC+0 |
| S3LOGS_STAGGING_MERGE_ORIG_BUCKETS | set to `true` if you want to merge different orig bucket log entries into one log file | true |
| S3LOGS_CONFIG_ROOT_PATH | location of parquet config files | /mnt/s3logs/config |
| S3LOGS_CONFIG_PARQUET_SCHEMA_FILE | name of log format parquet schema file | parquet.schema |
| S3LOGS_CONFIG_PARQUET_WRITER_PROPERTIES_FILE | name of parquet writer config file | parquet_writer_properties.ini |
| S3LOGS_TRANSFORM_ARCHIVE_ROOT_PATH | location of archive(processed) log files(gz) | /mnt/s3logs/archive |
| S3LOGS_TRANSFORM_PARQUET_ROOT_PATH | location of output parquet files | /mnt/s3logs/parquet |
| S3LOGS_TRANSFORM_OUTPUT_TARGET_PREFIX | prefix of S3's prefix to upload parquet | NULL |
| S3LOGS_TRANSFORM_OUTPUT_PREFIX_FMT | S3 prefix of parquet to be upload | year=%Y/month=%m/day=%d/hour=%H |
| S3LOGS_TRANSFORM_PARQUET_WRTIER_BULK_LINES | max lines of log which parquet writer batch once | 200000 |
| S3LOGS_TRANSFORM_JOB_INTERVAL | expiration time between transform jobs and last modification of stagging file (10 Min) | 600 |
| S3LOGS_TRANSFORM_AGGREGATE_SECOND | aggregate stagging files into time window (15 Min) **MUST > S3LOGS_STAGGING_PARTITION_SECOND** | 900 |
| S3LOGS_TRANSFORM_LOG_DEDUPLICATION | enable log entry deduplication | true |
| S3LOGS_TRANSFORM_CLEANUP_PROCESSED_LOGS | clean up processed log files</br>if set to `false`, processed log files will goes to `S3LOGS_TRANSFORM_ARCHIVE_ROOT_PATH` | true |
| S3LOGS_TRANSFORM_CLEANUP_UPLOADED_PARQUET | clean up uploaded parquet files</br>if set to `false`, parquet files will be kept in `S3LOGS_TRANSFORM_PARQUET_ROOT_PATH` | true |
| S3LOGS_TRANSFORM_STORAGE_CLASS | S3 storage class to use for upload parquet (STANDARD \| INTELLIGENT_TIERING) | STANDARD |
| S3LOGS_TRANSFORM_MPU_CHUNK_SIZE | chunk size of S3 multipart upload (5 MiB) | 5242880 |
| S3LOGS_FILE_BUF_SIZE | buffer size for both READ and WRITE when processing files (100 MiB) | 104857600 |
| S3LOGS_FILE_LOCK_TIMEOUT_SECONDS | timeout to try lock stagging file in seconds | 30 |
| S3LOGS_FILE_LOCK_RETRY_WAIT_MS | wait milliseconds for every file lock retry | 100 |

### Envroment that you DON'T want to change
| Environment | Description | Default |
| ----------- | ----------- | ------- |
| S3LOGS_STAGGING_FILE_DATETIME_FMT | name format for stagging files | %Y-%m-%d-%H-%M-%S%z |
| S3LOGS_STAGGING_FILE_SUFFIX | suffix of stagging file | .s3logs |
| S3LOGS_STAGGING_PROCESSING_SUFFIX | suffix of stagging file during transform | .processing |

## Security
See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License
This project is licensed under the Apache-2.0 License.
