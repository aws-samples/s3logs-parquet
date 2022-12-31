# FAQ
## Implementation
### Aggregator process incoming log files in parallel, will my log entries be overwritten in stagging files ?

**No**, aggregator will lock stagging files before append new log entries to prevent other append process, existed log entries will not be overwritten by new comings.

### In heavily workload, will file lock be a contention ?

**Yes**, file lock contention could happen in heavily log injection case, by change `S3LOGS_STAGGING_PARTITION_SECOND` to smaller granularity, file locks can be distributed across multiple stagging files, this can significant reduce file lock contention.

### Can transformer in parallel ?

**Yes**, transformer start from scan stagging files in stagging directory, this is in single main thread, as soon as main thread find all candidate stagging files, they will be grouped by original buckets (if you set `S3LOGS_STAGGING_MERGE_ORIG_BUCKETS` to `false`) and `S3LOGS_TRANSFORM_AGGREGATE_SECOND`, after that, each aggregation group will be spawned to new threads for merge and convert to parquet in parallel.

## s3logd
### If s3logd crashed or interrupted by user, any log entires will be lost ?

**No**, s3logd confidently delete SQS messages only when all log entries were written into stagging files, in any failure cases, original S3 event messages still exists in SQS, when s3logd recovered, failed messages will be retrieved and process again.

### If s3logd recovered after crash, will duplicate log entries exist in stagging files?

**Yes**, could have duplicate log entries exists in stagging files, as we talked before, in failure cases, partial log entries could have been written into stagging files while others not yet, that’s why transformer provide option `S3LOGS_TRANSFORM_LOG_DEDUPLICATION` to detect duplicate log entries, this feature will increase transformer CPU and Memory usage, but it worth.
