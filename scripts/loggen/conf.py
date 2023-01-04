HOUR = 3600
MINUTE = 60

# where to upload gen logs
REGION = 'your-aws-region'
BUCKET = 'your-log-destination-bucket'
PREFIX = 'your-log-prefix'

# per s3 log config
FILE_LOG_ENTRIES = 15000
FILE_TIME_RANGE = 10*MINUTE

# proc count start from
INIT_PROCS = 8
PROC_BULK_SIZE = 2
UPDATE_INTERVAL = 10

DRY_RUN = False

# target file per second
TARGET_FPS = 10
# target log files to gen
TARGET_LOG_FILES = 16000
