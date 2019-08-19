# job class
from datetime import datetime, timedelta, time
import csv
import numpy as np


file_name = 'wk1.log'

total_nodes = 49152
TOTAL_NODES = 49152 #MIRA
bounded_slowdown_threshold = 10.0 * 60.0  # 10 minutes in seconds

cobalt_partition_sizes = [512, 1024, 2048, 4096, 8192, 12288, 16384, 24576, 32768, 49152]

def main():
    jobs = parse_jobs(file_name)

    start_datetime1 = datetime(year=2018, month=1, day=19)
    end_datetime1 = start_datetime1 + timedelta(days=10)

    metrics_start_datetime1 = start_datetime1 + timedelta(days=2)
    metrics_end_datetime1 = metrics_start_datetime1 + timedelta(days=7)

    group_jobs_by_size(jobs, start_datetime1, end_datetime1, metrics_start_datetime1, metrics_end_datetime1)

    compute_job_metrics(jobs, start_datetime1, end_datetime1, metrics_start_datetime1, metrics_end_datetime1)

    print('\ndone!')


def compute_slowdown_walltime(job):
    bounded_runtime = max(10 * 60.0, job.RUNTIME_SECONDS)
    bounded_walltime = max(10 * 60.0, job.WALLTIME_SECONDS)

    slowdown = ((job.START_TIMESTAMP - job.QUEUED_TIMESTAMP).total_seconds() + bounded_runtime) / bounded_walltime
    return slowdown


def compute_slowdown_runtime(job):
    bounded_runtime = max(10 * 60.0, job.RUNTIME_SECONDS)

    slowdown = ((job.START_TIMESTAMP - job.QUEUED_TIMESTAMP).total_seconds() + bounded_runtime) / bounded_runtime
    return slowdown


def compute_runtime_walltime_ratio(job):
    bounded_runtime = max(10 * 60.0, job.RUNTIME_SECONDS)
    bounded_walltime = max(10 * 60.0, job.WALLTIME_SECONDS)

    return (bounded_runtime / bounded_walltime)


def group_jobs_by_size(jobs, start_datetime, end_datetime, metrics_start_datetime, metrics_end_datetime):
    # group jobs for all jobs in the window
    trimmed_job_count = 0


    # compute jobs by partition size
    jobs_by_partition_size = {}
    for partition_size in cobalt_partition_sizes:
        jobs_by_partition_size[partition_size] = []

    for job in jobs:
        if job.START_TIMESTAMP <= metrics_start_datetime or job.END_TIMESTAMP >= metrics_end_datetime:
            continue

        assert job.NODES_USED in cobalt_partition_sizes

        jobs_by_partition_size[job.NODES_USED].append(job)

        # print(job.WALLTIME_SECONDS / 60.0)

    for partition_size in cobalt_partition_sizes:
        print(str(partition_size) + ', ' + str(len(jobs_by_partition_size[partition_size])))


    for partition_size in cobalt_partition_sizes:
        print('\n-----')
        print(str(partition_size))
        print('-----')
        for job in jobs_by_partition_size[partition_size]:
            print(job.WALLTIME_SECONDS/60.0)


    print('done')
    # print('# jobs in window: ', str(trimmed_job_count))


def compute_job_metrics(jobs, start_datetime, end_datetime, metrics_start_datetime, metrics_end_datetime):
    jobs_in_window = []

    utilization_jobs = jobs

    slowdowns_walltime = []
    slowdowns_runtime = []
    turnaround_times = []
    runtimes = []
    walltimes = []

    runtime_walltime_ratios = []

    slowdown_walltime_category = {'narrow_short': [], 'narrow_long': [], 'wide_short': [], 'wide_long': []}
    slowdown_runtime_category = {'narrow_short': [], 'narrow_long': [], 'wide_short': [], 'wide_long': []}

    runtime_walltime_ratios_category = {'narrow_short': [], 'narrow_long': [], 'wide_short': [], 'wide_long': []}

    turnaround_times_category = {'narrow_short': [], 'narrow_long': [], 'wide_short': [], 'wide_long': []}
    category_count = {'narrow_short': 0, 'narrow_long': 0, 'wide_short': 0, 'wide_long': 0}

    # compute metrics for all jobs in the window
    for job in jobs:
        if job.START_TIMESTAMP <= metrics_start_datetime or job.END_TIMESTAMP >= metrics_end_datetime:
            continue

        slowdown_walltime = compute_slowdown_walltime(job)
        slowdown_runtime = compute_slowdown_runtime(job)

        runtime_walltime_ratio = compute_runtime_walltime_ratio(job)

        turnaround_time = (job.END_TIMESTAMP - job.QUEUED_TIMESTAMP).total_seconds()

        slowdowns_walltime.append(slowdown_walltime)
        slowdowns_runtime.append(slowdown_runtime)
        turnaround_times.append(turnaround_time)
        runtimes.append(job.RUNTIME_SECONDS)
        walltimes.append(job.WALLTIME_SECONDS)

        runtime_walltime_ratios.append(runtime_walltime_ratio)

        if job.NODES_USED <= 4096:  # if job is narrow
            if job.WALLTIME_SECONDS <= 120 * 60.0:  # if job is short
                job_category = 'narrow_short'
            else:  # if job is long
                job_category = 'narrow_long'
        else:  # if job is wide
            if job.RUNTIME_SECONDS <= 120 * 60.0:  # if job is short
                job_category = 'wide_short'
            else:  # if job is long
                job_category = 'wide_long'

        slowdown_walltime_category[job_category].append(slowdown_walltime)
        slowdown_runtime_category[job_category].append(slowdown_runtime)
        turnaround_times_category[job_category].append(turnaround_time)
        runtime_walltime_ratios_category[job_category].append(runtime_walltime_ratio)

        category_count[job_category] += 1

    avg_slowdown_walltime = sum(slowdowns_walltime) / float(len(slowdowns_walltime))
    avg_slowdown_runtime = sum(slowdowns_runtime) / float(len(slowdowns_runtime))

    avg_turnaround_time = sum(turnaround_times) / float(len(turnaround_times)) / 60.0
    avg_runtime = sum(runtimes) / float(len(runtimes)) / 60.0
    avg_walltime = sum(walltimes) / float(len(walltimes)) / 60.0

    avg_runtime_walltime_ratio = sum(runtime_walltime_ratios) / float(len(runtime_walltime_ratios))

    print('')
    print('system utilization % ' + str(get_utilization_over_window(utilization_jobs, start_datetime+timedelta(days=1), start_datetime+timedelta(days=6))))
    # print('system utilization % ' + str(get_utilization_over_window(jobs, start_datetime+timedelta(days=1), start_datetime+timedelta(days=6))))
    print('avg bounded slowdown walltime ' + str(avg_slowdown_walltime))
    print('avg bounded slowdown runtime ' + str(avg_slowdown_runtime))
    print('avg turnaround_times (min) ' + str(avg_turnaround_time))
    print('avg runtime (min) ' + str(avg_runtime))
    print('avg walltime (min) ' + str(avg_walltime))
    print('avg runtime-walltime ratio ' + str(avg_runtime_walltime_ratio))
    # print('max slowdown: ' + str(max(slowdowns)))

    print('')
    for category in ['narrow_short', 'narrow_long', 'wide_short', 'wide_long']:
        print(float(sum(slowdown_walltime_category[category])) / len(slowdown_walltime_category[category]))
        print(float(sum(slowdown_runtime_category[category])) / len(slowdown_runtime_category[category]))
        print(float(sum(turnaround_times_category[category])) / len(turnaround_times_category[category]) / 60.0)
        print(float(sum(runtime_walltime_ratios_category[category])) / len(runtime_walltime_ratios_category[category]))
        print(category_count[category])
        print('')



def list_utilization_by_weeks(jobs, start_datetime, end_datetime, increment):
    # start_datetime = datetime(year=2018, month=1, day=8)
    # end_datetime = datetime(year=2018, month=7, day=31)
    one_week = timedelta(days=7)
    # two_week = timedelta(days=14)
    one_day = timedelta(days=1)

    while start_datetime < end_datetime:
        tmp_utilization_week = get_utilization_over_window(jobs, start_datetime, start_datetime + one_week)
        # tmp_utilization_day = get_utilization_over_window(jobs, start_datetime, start_datetime + one_day)
        tmp_utilization_week = round(tmp_utilization_week, 2)
        # tmp_utilization_day = round(tmp_utilization_day, 2)
        utils_by_day_str = ',  utils by day: '
        for i in range(7):
            tmp = get_utilization_over_window(jobs, start_datetime+timedelta(days=i), start_datetime + timedelta(days=i+1))
            utils_by_day_str += str(round(tmp, 2)) + ', '

        print('week: ' + str(start_datetime.date()) + ' - util = ' + str(tmp_utilization_week) + utils_by_day_str)

        # print('One week ', (str(start_datetime),str(start_datetime + one_week), tmp_utilization))
        # print('just monday', get_utilization_over_window(jobs, start_datetime, start_datetime + one_day))
        # start_datetime += one_week
        start_datetime += increment

        # print()


def get_utilization_over_window(jobs, start_time, end_time):
    total_core_hours = 0.0
    for job in jobs:
        if job.START_TIMESTAMP >= start_time and job.END_TIMESTAMP <= end_time:
            total_core_hours += (job.END_TIMESTAMP-job.START_TIMESTAMP).total_seconds() * job.NODES_USED
        elif job.START_TIMESTAMP < start_time and job.END_TIMESTAMP > start_time and job.END_TIMESTAMP <= end_time:
            total_core_hours += (job.END_TIMESTAMP - start_time).total_seconds() * job.NODES_USED
        elif job.START_TIMESTAMP >= start_time and job.START_TIMESTAMP < end_time and job.END_TIMESTAMP > end_time:
            total_core_hours += (end_time - job.START_TIMESTAMP).total_seconds() * job.NODES_USED
    return total_core_hours / ((end_time - start_time).total_seconds() * total_nodes)


def get_utilization_at_time(jobs, current_time):
    total_nodes_used = 0.0
    for job in jobs:
        if job.START_TIMESTAMP <= current_time and job.END_TIMESTAMP >= current_time:
            total_nodes_used += job.NODES_USED
    return float(total_nodes_used) / total_nodes


def parseline(line):
    '''parse a line in work load file, return a temp
    dictionary with parsed fields in the line'''
    temp = {}
    firstparse = line.split(';')
    temp['EventType'] = firstparse[1]
    # if temp['EventType'] == 'Q':
    #     temp['submittime'] = firstparse[0]
    temp['jobid'] = firstparse[2]
    substr = firstparse.pop()
    if len(substr) > 0:
        secondparse = substr.split(' ')
        for item in secondparse:
            tup = item.partition('=')
            if not temp.has_key(tup[0]):
                temp[tup[0]] = tup[2]
    return temp


def parse_work_load(filename):
    '''parse the whole work load file, return a raw job dictionary'''
    temp = {'jobid':'*', 'submittime':'*', 'queue':'*',
            'Resource_List.walltime':'*','nodes':'*', 'runtime':'*'}
    # raw_job_dict = { '<jobid>':temp, '<jobid2>':temp2, ...}
    raw_job_dict = {}
    wlf = open(filename, 'r')
    for line in wlf:
        line = line.strip('\n')
        line = line.strip('\r')
        if line[0].isdigit():
            temp = parseline(line)
        else:
            # temp = parseline_alt(line)
            print('error')
            exit(-1)
        jobid = temp['jobid']
        #new job id encountered, add a new entry for this job
        if not raw_job_dict.has_key(jobid):
            raw_job_dict[jobid] = temp
        else:  #not a new job id, update the existing entry
            raw_job_dict[jobid].update(temp)

    return raw_job_dict


def parse_jobs(workload_file):
    raw_jobs = parse_work_load(workload_file)

    specs = []

    jobs_log_values = {}
    jobs_list = []

    tag = 0
    temp_num = 0
    for key in raw_jobs:
        # __0508:
        temp_num = temp_num + 1
        # print "[Init] temp_num: ", temp_num
        # _0508
        spec = {}
        tmp = raw_jobs[key]
        spec['jobid'] = tmp.get('jobid')
        spec['queue'] = tmp.get('queue')
        spec['user'] = tmp.get('user')

        if tmp.get('qtime'):
            qtime = float(tmp.get('qtime'))
        else:
            continue
        # if qtime < self.sim_start or qtime > self.sim_end:
        #     continue
        spec['submittime'] = qtime
        # spec['submittime'] = float(tmp.get('qtime'))
        spec['first_subtime'] = spec['submittime']  # set the first submit time

        # spec['user'] = tmp.get('user')
        spec['project'] = tmp.get('account')

        # convert walltime from 'hh:mm:ss' to float of minutes
        format_walltime = tmp.get('Resource_List.walltime')
        spec['walltime'] = 0
        if format_walltime:
            segs = format_walltime.split(':')
            walltime_minuntes = int(segs[0]) * 60 + int(segs[1])
            spec['walltime'] = str(int(segs[0]) * 60 + int(segs[1]))
        else:  # invalid job entry, discard
            continue

        if tmp.get('runtime'):
            spec['runtime'] = tmp.get('runtime')
        elif tmp.get('start') and tmp.get('end'):
            act_run_time = float(tmp.get('end')) - float(tmp.get('start'))
            if act_run_time <= 0:
                continue
            if act_run_time / (float(spec['walltime']) * 60) > 1.1:
                act_run_time = float(spec['walltime']) * 60
            spec['runtime'] = str(round(act_run_time, 1))
        else:
            continue

        if tmp.get('Resource_List.nodect'):
            spec['nodes'] = tmp.get('Resource_List.nodect')
            if int(spec['nodes']) == TOTAL_NODES:
                continue
        else:  # invalid job entry, discard
            continue

        # if self.walltime_prediction:  # *AdjEst*
        #     if tmp.has_key('walltime_p'):
        #         spec['walltime_p'] = int(
        #             tmp.get('walltime_p')) / 60  # convert from sec (in log) to min, in line with walltime
        #     else:
        #         ap = self.get_walltime_Ap(spec)
        #         spec['walltime_p'] = int(spec['walltime']) * ap
        # else:
        spec['walltime_p'] = int(spec['walltime'])

        spec['state'] = 'invisible'
        spec['start_time'] = '0'
        spec['end_time'] = '0'
        # spec['queue'] = "default"
        spec['has_resources'] = False
        spec['is_runnable'] = False
        spec['location'] = tmp.get('exec_host', '')  # used for reservation jobs only
        spec['start_time'] = tmp.get('start', 0)  # used for reservation jobs only
        # dwang:
        spec['restart_overhead'] = 0.0
        #

        job_values = {}
        job_values['queued_time'] = float(spec['submittime'])
        job_values['log_start_time'] = float(tmp.get('start'))
        job_values['log_end_time'] = float(tmp.get('end'))
        job_values['log_run_time'] = float(spec['runtime']) / 60.0
        temp_bounded_runtime = max(float(spec['runtime']), bounded_slowdown_threshold)
        job_values['log_slowdown'] = (job_values['log_start_time'] - job_values['queued_time'] +
                                      temp_bounded_runtime) / temp_bounded_runtime
        job_values['log_turnaround_time'] = (job_values['log_end_time'] - job_values['queued_time']) / 60.0
        job_values['log_queue_time'] = (job_values['log_start_time'] - job_values['queued_time']) / 60.0
        job_values['nodes'] = int(spec['nodes'])
        if 'utilization_at_queue_time' in tmp:
            job_values['log_utilization_at_queue_time'] = float(tmp.get('utilization_at_queue_time'))
        else:
            job_values['log_utilization_at_queue_time'] = -1.0

        jobs_log_values[int(spec['jobid'])] = job_values

        new_job = job(int(spec['jobid']), float(spec['submittime']), float(tmp.get('start')), float(tmp.get('end')),
                      float(spec['runtime']), int(spec['nodes']), float(spec['walltime'])*60.0 )
        jobs_list.append(new_job)

        # add the job spec to the spec list
        specs.append(spec)
    return jobs_list


class job:
    def __init__(self, COBALT_JOBID, QUEUED_TIMESTAMP, START_TIMESTAMP, END_TIMESTAMP, RUNTIME_SECONDS, NODES_USED, WALLTIME_SECONDS):
        # idx = 0
        # self.JOB_NAME = entry[idx]
        # idx += 1
        self.COBALT_JOBID = COBALT_JOBID
        # idx += 1
        # self.MACHINE_NAME = entry[idx]
        # idx += 1
        self.QUEUED_TIMESTAMP =  datetime.utcfromtimestamp(QUEUED_TIMESTAMP)
        self.START_TIMESTAMP = datetime.utcfromtimestamp(START_TIMESTAMP)
        self.END_TIMESTAMP = datetime.utcfromtimestamp(END_TIMESTAMP)
        # self.QUEUED_TIMESTAMP = QUEUED_TIMESTAMP
        # self.START_TIMESTAMP = START_TIMESTAMP
        # self.END_TIMESTAMP = END_TIMESTAMP
        self.RUNTIME_SECONDS = RUNTIME_SECONDS
        self.NODES_USED = NODES_USED
        self.WALLTIME_SECONDS = WALLTIME_SECONDS
        # try:
        #     self.QUEUED_TIMESTAMP = datetime.strptime(entry[idx], '%Y-%m-%d %H:%M:%S')
        # except:
        #     self.QUEUED_TIMESTAMP = datetime.strptime(entry[idx], '%Y-%m-%d %H:%M:%S.%f')
        # idx += 1
        # self.QUEUED_DATE_ID = int(entry[idx])
        # idx += 1
        # try:
        #     self.START_TIMESTAMP = datetime.strptime(entry[idx], '%Y-%m-%d %H:%M:%S')
        # except:
        #     self.START_TIMESTAMP = datetime.strptime(entry[idx], '%Y-%m-%d %H:%M:%S.%f')
        # idx += 1
        # self.START_DATE_ID = int(entry[idx])
        # idx += 1
        # try:
        #     self.END_TIMESTAMP = datetime.strptime(entry[idx], '%Y-%m-%d %H:%M:%S')
        # except:
        #     self.END_TIMESTAMP = datetime.strptime(entry[idx], '%Y-%m-%d %H:%M:%S.%f')
        # idx += 1
        # self.END_DATE_ID = int(entry[idx])
        # idx += 1
        # self.USERNAME_GENID = entry[idx]
        # idx += 1
        # self.PROJECT_NAME_GENID = entry[idx]
        # idx += 1
        # self.QUEUE_NAME = entry[idx]
        # idx += 1
        # self.WALLTIME_SECONDS = float(entry[idx])
        # idx += 1
        # self.RUNTIME_SECONDS = float(entry[idx])
        # idx += 1
        # self.NODES_USED = float(entry[idx])
        # idx += 1
        # self.NODES_REQUESTED = float(entry[idx])
        # idx += 1
        # self.CORES_USED = float(entry[idx])
        # idx += 1
        # self.CORES_REQUESTED = float(entry[idx])
        # idx += 1
        # self.LOCATION = entry[idx]
        # idx += 1
        # self.EXIT_STATUS = int(entry[idx])
        # idx += 1
        # self.ELIGIBLE_WAIT_SECONDS = int(entry[idx])
        # idx += 1
        # self.ELIGIBLE_WAIT_FACTOR = int(entry[idx])
        # idx += 1
        # self.QUEUED_WAIT_SECONDS = int(entry[idx])
        # idx += 1
        # self.QUEUED_WAIT_FACTOR = int(entry[idx])
        # idx += 1
        # self.REQUESTED_CORE_HOURS = float(entry[idx])
        # idx += 1
        # self.USED_CORE_HOURS = float(entry[idx])
        # idx += 1
        # self.CAPABILITY_USAGE_CORE_HOURS = float(entry[idx])
        # idx += 1
        # self.NONCAPABILITY_USAGE_CORE_HOURS = float(entry[idx])
        # idx += 1
        # self.BUCKETS3_A_USAGE_CORE_HOURS = float(entry[idx])
        # idx += 1
        # self.BUCKETS3_B_USAGE_CORE_HOURS = float(entry[idx])
        # idx += 1
        # self.BUCKETS3_C_USAGE_CORE_HOURS = float(entry[idx])
        # idx += 1
        # self.MACHINE_PARTITION = entry[idx]
        # idx += 1
        # self.EXIT_CODE = int(entry[idx])
        # idx += 1
        # self.MODE = entry[idx]
        # idx += 1
        # self.RESID = int(entry[idx])
        # idx += 1
        # self.DATA_LOAD_STATUS = entry[idx]
        # idx += 1
        # self.CAPABILITY = entry[idx]
        # idx += 1
        # self.SIZE_BUCKETS3 = entry[idx]
        # idx += 1
        # self.PERCENTILE = entry[idx]
        # idx += 1
        # self.NUM_TASKS_SUBBLOCK = int(entry[idx])
        # idx += 1
        # self.NUM_TASKS_CONSECUTIVE = int(entry[idx])
        # idx += 1
        # self.NUM_TASKS_MULTILOCATION = int(entry[idx])
        # idx += 1
        # self.NUM_TASKS_SINGLE = int(entry[idx])
        # idx += 1
        # self.COBALT_NUM_TASKS = int(entry[idx])
        # idx += 1
        # self.IS_SINGLE = int(entry[idx])
        # idx += 1
        # self.IS_CONSECUTIVE = int(entry[idx])
        # idx += 1
        # self.IS_MULTILOCATION = int(entry[idx])
        # idx += 1
        # self.IS_SUBBLOCK = int(entry[idx])
        # idx += 1
        # self.IS_SUBBLOCK_ONLY = int(entry[idx])
        # idx += 1
        # self.IS_MULTILOCATION_ONLY = int(entry[idx])
        # idx += 1
        # self.IS_MULTILOCATION_SUBBLOCK = int(entry[idx])
        # idx += 1
        # self.IS_CONSECUTIVE_ONLY = int(entry[idx])
        # idx += 1
        # self.IS_SINGLE_ONLY = int(entry[idx])
        # idx += 1
        # self.IS_NO_TASKS = int(entry[idx])
        # idx += 1
        # self.IS_OTHER = int(entry[idx])
        # idx += 1
        # self.OVERBURN_CORE_HOURS = float(entry[idx])
        # idx += 1
        # self.IS_OVERBURN = int(entry[idx])
        # idx += 1
        #
        # self.UTILIZATION_AT_QUEUE_TIME = -1.0


if __name__== "__main__":
  main()