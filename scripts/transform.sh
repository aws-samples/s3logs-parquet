#!/bin/bash
#
# modify below params before you go
#################################################
REGION='your-aws-region'
BUCKET='your-ouput-parquet-bucket'
TOPPATH='your-local-bin-dir'
#
# script tunables
# every 60s start one new thread
max_threads=1 # max threads each time
interval=60 # loop check interval 60s
##################################################

# get system CPUs
cpus=`nproc --all`

function run {
  export S3LOGS_STAGGING_PARTITION_TZIF=UTC+8
  export S3LOGS_TRANSFORM_JOB_INTERVAL=300
  export S3LOGS_TRANSFORM_AGGREGATE_SECOND=300
  export S3LOGS_TRANSFORM_OUTPUT_PREFIX_FMT=year=%Y/month=%m/day=%d
  export S3LOGS_TRANSFORM_OUTPUT_TARGET_PREFIX=s3logs
  dt=`date '+%F %H:%M:%S'`
  echo "$dt - run with s3logs transform with $1 threads"
  $TOPPATH/s3logs transform -r $REGION -b $BUCKET -t $1 >> $TOPPATH/transform.log 2>&1 &
}

function calc_running {
  total=0
  pids=`pgrep -f ".*s3logs transform.*"`
  for pid in $pids; do
    thr=`ps --no-headers -L $pid | wc -l`
    # minus 1 for main thread
    let total=total+thr-1
  done

  if [ $total -lt 0 ]; then
    return 0
  else
    return $total
  fi
}

function main_loop {
  echo "$cpus of CPUs available in this system, max threads for transformer $max_threads"
  while true; do
    available=0
    calc_running
    running=$?
    let available=$cpus-$running
    if [ $available -gt 0 ]; then
      if [ $available -ge $max_threads ]; then
        run $max_threads
      else
        run $available
      fi
    else
      dt=`date '+%F %H:%M:%S'`
      echo "$dt - no cpu core available, skip this cycle"
    fi
    sleep $interval
  done
}

main_loop
