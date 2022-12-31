# s3log transformer lambda
s3log transformer provide cost optimization way to run aggregated S3 logs conversion to parquet in serverless.

In case your S3 logs **NOT** massive, serverless would be best fit.

## Read aggregation results from EFS
Since lambda itself **DO NOT** provide data persistence accross it's execution lifetime, serverless deployment of s3log both aggregator and transformor need a external shared persistence data storage, we use cloud native [Amazon EFS](https://docs.aws.amazon.com/efs/index.html)

To mount a EFS share during lambda boot, you need to deploy lambda into a VPC.

See [Using Amazon EFS for AWS Lambda in your serverless applications](https://aws.amazon.com/blogs/compute/using-amazon-efs-for-aws-lambda-in-your-serverless-applications/) 

More details at AWS Documents:

- [Getting started with Amazon Elastic File System](https://docs.aws.amazon.com/efs/latest/ug/getting-started.html)
- [Configuring file system access for Lambda functions](https://docs.aws.amazon.com/lambda/latest/dg/configuration-filesystem.html)

## Configure lambda trigger
A external trigger needed to schedule s3log transformer lambda in a fixed time interval, see [Schedule AWS Lambda Functions Using CloudWatch Events](https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/RunLambdaSchedule.html)

## Build and Deploy Lambda
You need to build and package rust binary locally before deploy as a lambda

### Method 1. Build and package lambda
Following the steps in [Package and upload the app](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/lambda.html#lambda-step3).

### Method 2. Use cargo-lambda
cargo-lambda helps you easily build and deploy lambda with Rust code.

Basically you need:
```
pip3 install cargo-lambda
cargo lambda build --release
cargo lambda deploy
```
Check [Installation](https://www.cargo-lambda.info/guide/installation.html) and [Getting Started](https://www.cargo-lambda.info/guide/getting-started.html) for more details.

## Set Environments
See [Environment settings](../README.md#environment-settings) for all tunnables.
