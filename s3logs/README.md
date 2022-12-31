# Aggregate and transform AWS S3 server access logs to parquet

## Use CLI

### For Aggregation Stage

```
# process logs from your local file system
$ cargo run --release -- aggregate local <input>
```
```
# process logs from S3
$ cargo run --release -- aggregate s3 -r <region> -b <bucket> -k <key>
```

### For Transformation Stage
```
# convert logs to parquet
# supply [input] if you want to convert specific file(s) or it will scan S3LOGS_STAGGING_ROOT_PATH for candidate
$ cargo run --release -- transform -r <region> -b <bucket> [input]
```

### Enable DEBUG
default log level is set to INFO, you can enable DEBUG by run:
```
export RUST_LOG=s3logs=debug
```

## Set Environments
See [Environment settings](../README.md#environment-settings) for all tunnables.
