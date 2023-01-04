#
# Amazon S3 fake server access log generator
#
import os
import sys
from datetime import datetime
from datetime import timezone
import time
from random import choice
import string
import random
import base64
import boto3
import queue
import multiprocessing as mp
import conf

os.environ['TZ'] = 'UTC'

# https://stackoverflow.com/questions/42707878/amazon-s3-logs-operation-definition
ops = [
    'REST.HEAD.OBJECT', 'REST.GET.OBJECT', 'REST.PUT.OBJECT',
    'REST.DELETE.OBJECT', 'REST.COPY.OBJECT',
    'REST.GET.UPLOAD', 'REST.PUT.PART'
]

BUCKET = 'fakebucket'
FOLDER_LEVEL = (3, 8)
FOLDER_NAME = (10, 25)

def gen_user_agent(rand):
    if rand:
        return '"curl/7.15.1 with test string {}"'.format(random_str(50, False))
    else: # fixed
        return '"S3Console/0.4, aws-internal/3 aws-sdk-java/1.11.1030 Linux/5.10.144-111.639.amzn2int.x86_64 OpenJDK_64-Bit_Server_VM/25.352-b08 java/1.8.0_352 vendor/Oracle_Corporation cfg/retry-mode/standard"'

def random_bucket_name():
        return ''.join(choice(string.ascii_lowercase + string.digits) for _ in range(20, 64))

def random_str(length, uppercase):
    if uppercase:
        return ''.join(choice(string.ascii_uppercase + string.digits) for _ in range(length))
    else:
        return ''.join(choice(string.ascii_letters + string.digits) for _ in range(length))

def generate_random_folder():
    return '/'.join(random_str(choice(FOLDER_LEVEL), False) for _ in range(choice(FOLDER_LEVEL)))

def generate_random_object(length):
    return random_str(length, False)

class S3LogEntry:

    def __init__(self, orig_bucket, ts):
        self.prefix = generate_random_folder()

        self.bucket_owner = 'deadbeef00b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be'
        self.bucket = orig_bucket
        self.time = '[' + datetime.fromtimestamp(ts, timezone.utc).strftime("%d/%b/%Y:%H:%M:%S %z") + ']'
        self.remoteip = '192.0.{}.{}'.format(choice(range(0, 255)), choice(range(0, 255)))
        self.requester = 'deadbeef00b949e55d96a1e698fbacedfd6e09d98eacf8f8d5218e7cd47ef2be'
        self.requestid = random_str(16, True)
        self.operation = choice(ops)
        self.key = '/{}/{}.txt'.format(self.prefix, generate_random_object(10))
        self.request_uri = '"{} /{}{} HTTP/1.1"'.format(choice(['GET', 'PUT', 'POST']), self.bucket, self.key)
        self.http_status = choice(['200', '404', '503'])
        self.error_code = choice(['-', 'NoSuchKey'])
        self.bytes_sent = str(random.randrange(0, 1000000000000))
        self.object_size = str(random.randrange(0, 1000000000000))
        self.total_time = str(random.randrange(10, 200))
        self.turn_around_time = str(random.randrange(10, 200))
        self.referer = '"http://www.amazon.com/webservices"'
        self.user_agent = gen_user_agent(False)
        self.version_id = '-'
        self.host_id = base64.b64encode(random_str(50, False).encode()).decode()
        self.sig = 'SigV4'
        self.cipher = 'ECDHE-RSA-AES128-GCM-SHA256'
        self.auth_type = 'AuthHeader'
        self.host_header = 's3.us-west-2.amazonaws.com'
        self.tls_version = 'TLSv1.2'
        self.ap_arn = '-'
        self.acl_required = '-'

    def __str__(self):
        return " ".join([self.bucket_owner, self.bucket, self.time, self.remoteip, self.requester,
            self.requestid, self.operation, self.key, self.request_uri, self.http_status, self.error_code,
            self.bytes_sent, self.object_size, self.total_time, self.turn_around_time, self.referer,
            self.user_agent, self.version_id, self.host_id, self.sig, self.cipher, self.auth_type,
            self.host_header, self.tls_version, self.ap_arn, self.acl_required])

class S3LogObject:
    def __init__(self):
        #self.orig_bucket = random_bucket_name()
        self.orig_bucket = BUCKET
        self.start = datetime.utcnow()
        self.start_ts = int(self.start.timestamp())
        self.filename = self.start.strftime("%Y-%m-%d-%H-%M-%S") + '-{}'.format(random_str(16, True))

    # params:
    #   @lines - how many lines of log entry in this file
    #   @backseconds - random time range from utc now this file covered
    def gen(self, lines, backseconds):
        with open(self.filename, 'w+') as f:
            for i in range(0, lines):
                ts = random.randrange(self.start_ts - backseconds, self.start_ts)
                f.write('{}\n'.format(S3LogEntry(self.orig_bucket, ts)))

    def upload(self, client):
        with open(self.filename, 'rb') as data:
            if conf.PREFIX == '':
                key = self.filename
            else:
                key = "{}/{}".format(conf.PREFIX, self.filename)

            client.upload_fileobj(data, conf.BUCKET, key)

    def remove(self):
        if os.path.isfile(self.filename):
            os.remove(self.filename)
        else:
            print("file {} not exist".format(self.filename))

def proc_func(res_queue, task_queue, proc_count, proc_id, dry_run):

    #print("  proc {} STARTED".format(proc_id))
    local_proc_id = proc_id
    s3 = boto3.client('s3')
    while True:
        try:
            bulk_size = task_queue.get(True, conf.UPDATE_INTERVAL/2)
        except queue.Empty:
            #print("  no more works, proc {} EXIT".format(local_proc_id))
            sys.exit(1)

        start = time.time_ns()
        for i in range(0, bulk_size):
            file = S3LogObject()
            file.gen(conf.FILE_LOG_ENTRIES, conf.FILE_TIME_RANGE)
            if not dry_run:
                file.upload(s3)
            file.remove()
        cost = time.time_ns() - start

        #print("  -> proc id {}, files: {}, cost: {:.2f}".format(proc_id, bulk_size, cost/1000_000_000))
        res_queue.put((bulk_size, cost))
        if proc_count.value < local_proc_id:
            #print("  proc {} EXIT".format(local_proc_id))
            sys.exit(1)

class S3LogReactor:

    def __init__(self):
        self.os_cpus = os.cpu_count()
        self.init_procs = conf.INIT_PROCS
        self.bulk_size = conf.PROC_BULK_SIZE
        self.res_queue = mp.Queue()
        self.task_queue = mp.Queue()
        self.target_fps = conf.TARGET_FPS
        self.cum_interval =  conf.UPDATE_INTERVAL
        self.proc_count = mp.Value('i', 1)
        self.completed = 0

        print("S3LogReactor init")
        print(" - init procs {}, OS CPUs {}".format(self.init_procs, self.os_cpus))
        print(" - bulk size {}, updte interval {}s".format(conf.PROC_BULK_SIZE, conf.UPDATE_INTERVAL))
        print(" - target fps {}, target log files {}".format(conf.TARGET_FPS, conf.TARGET_LOG_FILES))

    def child_count(self):
        return len(mp.active_children())
 
    def update_stats(self):

        results = list()
        total_files = 0
        total_cost = 0
        while True:
            try:
                (files, cost) = self.res_queue.get_nowait()
                total_files = total_files + files
                total_cost = total_cost + cost
            except queue.Empty:
                break

        if self.task_queue.qsize() == 0: 
            total_s = (time.time_ns() - self.start)/1000_000_000
            print("overall time used {:.2f}s, {} files generated".format(total_s, conf.TARGET_LOG_FILES))
            print("all tasks done, wait reactor to quit")
            sys.exit(0)

        if total_files == 0 and total_cost == 0:
            #print("{} - active procs: {} target_fps: {}, skip...".format(time.strftime('%X'), self.child_count(), self.target_fps))
            self.cum_interval = self.cum_interval + conf.UPDATE_INTERVAL
            return

        fps = total_files/self.cum_interval
        per_file_cost = total_cost/1000_000_000/total_files
        if round(fps) > self.target_fps:
            self.proc_count.value = self.proc_count.value - 1
        elif round(fps) < self.target_fps:
            if self.proc_count == self.os_cpus:
                print("MAX proc count {} reached".format(self.proc_count.value))
            else:
                proc_id = self.proc_count.value
                p = mp.Process(target=proc_func, args=(self.res_queue, self.task_queue, self.proc_count, proc_id, conf.DRY_RUN))
                p.start()
                self.proc_count.value = self.proc_count.value + 1

        self.completed = self.completed + total_files
        print("{} - {:>5.2f}% active procs: {:>2}, fps: {:>5.2f}, target_fps: {:>2}, time cost per file: {:>5.2f}s".format(
            time.strftime('%X'), self.completed*100.0/conf.TARGET_LOG_FILES, self.child_count(), fps, self.target_fps, per_file_cost)
        )
        self.cum_interval =  conf.UPDATE_INTERVAL

    def run(self):

        # allocate task queue
        left = conf.TARGET_LOG_FILES
        while left > 0:
            if left > conf.PROC_BULK_SIZE:
                self.task_queue.put(conf.PROC_BULK_SIZE)
                left = left - conf.PROC_BULK_SIZE
            else:
                self.task_queue.put(left)
                left = 0

        self.start = time.time_ns()

        # init base procs
        for i in range(self.init_procs):
            proc_id = self.proc_count.value
            p = mp.Process(target=proc_func, args=(self.res_queue, self.task_queue, self.proc_count, proc_id, conf.DRY_RUN))
            p.start()
            self.proc_count.value = self.proc_count.value + 1

        while True:
            time.sleep(conf.UPDATE_INTERVAL)
            self.update_stats()

if __name__ == "__main__":
    mp.set_start_method('spawn')
    reactor = S3LogReactor()
    reactor.run()
