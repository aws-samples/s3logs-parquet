# s3logs in stream mode

## How to use
### Polling Amazon SQS and basic operation

please refer to [How to use](../s3logd/README.md#how-to-use)

basically s3logd-stream shared same SQS handling logic and operation model with s3logd.

except one option:

```num_executors``` to control the number of concurrent parquet output tasks

### Output Section

s3logd-stream moved all configuration parameters into config file, checkout ```[OUTPUT]``` section for all tunnables.

***WARNING*** currently only utc+0 date partition tested and supported.

to use utc+0 date partition, be sure:

#### 1. set following in your config file.

```
hourly_partition = false
timezone = UTC+0
event_time_key_format = true
```

#### 2. config your S3 access log in event time delivery

refer Amazon S3 public doc for how to config [event time delivery](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerLogs.html#server-access-logging-overview)

after config event time delivery, you will receive S3 access logs in below format:

```logs/123456789012/us-west-2/DOC-EXAMPLE-SOURCE-BUCKET/2023/03/01/2023-03-01-00-00-00-E568B2907131C0C0```
