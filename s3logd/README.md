# s3logs aggregator daemon

## How to use
### Polling Amazon SQS
s3logd polling [Amazon SQS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html) for [new object create events](https://docs.aws.amazon.com/AmazonS3/latest/userguide/NotificationHowTo.html) messages when new S3 log object delivered.

### Configure a S3 bucket event notfication for SQS
Before you start s3logd, be sure you have created and configured S3 event notification ```s3:ObjectCreated:Put``` and ```s3:ObjectCreated:CompleteMultipartUpload``` for SQS.

See [Configuring a bucket for notifications (SNS topic or SQS queue)](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ways-to-add-notification-config-to-bucket.html).

### Configuration
s3logd load config from local file, parameters explains below:

| PARAMETER | DESCRIPTION | DEFAULT |
| --- | --- | --- |
| region | region of SQS to get event | N/A |
| queue | SQS queue url<br/>example: ```https://sqs.us-west-1.amazonaws.com/111111111111/s3logsq``` | N/A |
| loglevel | loglevel setting of s3logd | s3logd=info,s3logs=info |
| logfile | log messages destination | s3logd.log |
| max_sqs_messages | max number of messages to return<br/>see [MaxNumberOfMessages](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html) | 10 |
| sqs_wait_time_seconds | SQS long polling wait time (seconds)<br/>see [Consuming messages using long polling](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html#sqs-long-polling) | 20 |
| sqs_poll_idle_seconds | when NO new messages received at sqs_wait_time_seconds polling, how many seconds  to wait for another polling | 1 |
| num_workers | number of worker threads to handle incomming new s3 events | 4 |
| max_recv_queue_len | size of in memory message queue to hold unpreocess S3 events | 10 |

### Start s3logd daemon
```
$ cargo run --release -- -c config.ini -d
```

### Start s3logd in foreground
```
$ cargo run --release -- -c config.ini
```

### Graceful shutdown daemon
To ensure all inflight events be processed before quit,
use following command to shutdown s3logd gracefully.
```
$ kill -SIGHUP `cat s3logd.pid`
```

## Set Environments
See [Environment settings](../README.md#environment-settings) for all tunnables.
