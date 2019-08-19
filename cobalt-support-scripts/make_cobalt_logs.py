# job class
from datetime import datetime, timedelta, time
import csv
import numpy as np

# csv_file_name = 'ANL-ALCF-DJC-MIRA_20170101_20171231.csv'
csv_file_name = 'ANL-ALCF-DJC-MIRA_20180101_20180731.csv'

total_nodes = 49152


def main():
    jobs = parse_jobs()

    # test_over_100_utilization(jobs)
    # test_weird_utilization(jobs, start_datetime=datetime(2018,06,9))

    # start_datetime = datetime(year=2017, month=1, day=2)
    # end_datetime = datetime(year=2017, month=12, day=31)

    start_datetime = datetime(year=2018, month=1, day=7)
    end_datetime = datetime(year=2018, month=7, day=31)
    one_week = timedelta(days=7)
    two_week = timedelta(days=14)

    # list_utilization_by_weeks(jobs, start_datetime, end_datetime, two_week)
    # return

    # start_datetime1 = datetime(year=2018, month=1, day=20)
    # end_datetime1 = start_datetime1 + timedelta(days=9)
    # metrics_start_datetime = datetime(year=2018, month=1, day=22)
    # metrics_end_datetime = metrics_start_datetime + timedelta(days=5)
    # output_log_name = 'wk1-lowSD.log'
    # make_cobalt_logs(jobs, start_datetime1, end_datetime1, metrics_start_datetime, metrics_end_datetime,
    #                  output_log_name, slowdownThreshold=True)

    start_datetime1 = datetime(year=2018, month=1, day=19)
    end_datetime1 = start_datetime1 + timedelta(days=10)

    metrics_start_datetime1 = start_datetime1 + timedelta(days=2)
    metrics_end_datetime1 = metrics_start_datetime1 + timedelta(days=7)

    start_datetime2 = datetime(year=2018, month=3, day=16)
    end_datetime2 = start_datetime2 + timedelta(days=10)
    
    metrics_start_datetime2 = start_datetime2 + timedelta(days=2)
    metrics_end_datetime2 = metrics_start_datetime2 + timedelta(days=7)

    start_datetime3 = datetime(year=2018, month=6, day=8)
    end_datetime3 = start_datetime3 + timedelta(days=10)
    
    metrics_start_datetime3 = start_datetime3 + timedelta(days=2)
    metrics_end_datetime3 = metrics_start_datetime3 + timedelta(days=7)

    start_datetime4 = datetime(year=2018, month=3, day=02)
    end_datetime4 = start_datetime4 + timedelta(days=10)

    metrics_start_datetime4 = start_datetime4 + timedelta(days=2)
    metrics_end_datetime4 = metrics_start_datetime4 + timedelta(days=7)

    # slowdownThreshold = True
    slowdownThreshold = False

    if slowdownThreshold is False:
        output_log_name ='wk1.log'
        make_cobalt_logs(jobs, start_datetime1, end_datetime1, metrics_start_datetime1, metrics_end_datetime1,
                                          output_log_name, slowdownThreshold=False)
        # make_cobalt_logs(jobs, start_datetime1, end_datetime1, output_log_name, slowdownThreshold=True)

        output_log_name ='wk2.log'
        make_cobalt_logs(jobs, start_datetime2, end_datetime2, metrics_start_datetime2, metrics_end_datetime2,
                                          output_log_name, slowdownThreshold=False)
        # make_cobalt_logs(jobs, start_datetime2, end_datetime2, output_log_name, slowdownThreshold=True)

        output_log_name ='wk3.log'
        make_cobalt_logs(jobs, start_datetime3, end_datetime3, metrics_start_datetime3, metrics_end_datetime3,
                                          output_log_name, slowdownThreshold=False)
        # make_cobalt_logs(jobs, start_datetime3, end_datetime3, output_log_name, slowdownThreshold=True)

        output_log_name ='wk4.log'
        make_cobalt_logs(jobs, start_datetime4, end_datetime4, metrics_start_datetime4, metrics_end_datetime4,
                                          output_log_name, slowdownThreshold=False)
        # make_cobalt_logs(jobs, start_datetime3, end_datetime3, output_log_name, slowdownThreshold=True)

    else:
        output_log_name ='wk1-lowSD.log'
        make_cobalt_logs(jobs, start_datetime1, end_datetime1, metrics_start_datetime1, metrics_end_datetime1,
                                          output_log_name, slowdownThreshold=True)
        # make_cobalt_logs(jobs, start_datetime1, end_datetime1, output_log_name, slowdownThreshold=True)

        output_log_name ='wk2-lowSD.log'
        make_cobalt_logs(jobs, start_datetime2, end_datetime2, metrics_start_datetime2, metrics_end_datetime2,
                                          output_log_name, slowdownThreshold=True)
        # make_cobalt_logs(jobs, start_datetime2, end_datetime2, output_log_name, slowdownThreshold=True)

        output_log_name ='wk3-lowSD.log'
        make_cobalt_logs(jobs, start_datetime3, end_datetime3, metrics_start_datetime3, metrics_end_datetime3,
                                          output_log_name, slowdownThreshold=True)
        # make_cobalt_logs(jobs, start_datetime3, end_datetime3, output_log_name, slowdownThreshold=True)

    print('\ndone!')
    # week 2018-01-22 (81%), 2018-03-19 (89%), 2018-06-11 (95%)  


def compute_slowdown(job):
    bounded_run_time = max(10 * 60.0, job.RUNTIME_SECONDS)
    # slowdown = ((
    #                 job.START_TIMESTAMP - job.QUEUED_TIMESTAMP).total_seconds() + bounded_run_time) / bounded_run_time
    slowdown = (job.ELIGIBLE_WAIT_SECONDS + bounded_run_time) / bounded_run_time
    return slowdown

def compute_turnaround_time(job):
    return (job.ELIGIBLE_WAIT_SECONDS + job.RUNTIME_SECONDS)

    
def make_cobalt_logs(jobs, start_datetime, end_datetime, metrics_start_datetime, metrics_end_datetime, output_log_name, slowdownThreshold=False):
    jobs_in_window = []
    
    # for job in jobs:
    #     if job.COBALT_JOBID == 1449589:
    #         pass

    # update queued_timestamps
    for job in jobs:
        job.QUEUED_TIMESTAMP = job.START_TIMESTAMP - timedelta(seconds=float(job.ELIGIBLE_WAIT_SECONDS))

    count1, count2, count3, count4 = 0, 0, 0, 0
    for job in jobs:
        if job.QUEUED_TIMESTAMP >= start_datetime and job.QUEUED_TIMESTAMP <= end_datetime:
            jobs_in_window.append(job)
            count1 += 1
        elif job.END_TIMESTAMP >= start_datetime and job.END_TIMESTAMP <= end_datetime:
            jobs_in_window.append(job)
            count2 += 1
        elif job.START_TIMESTAMP >= start_datetime and job.START_TIMESTAMP <= end_datetime:
            jobs_in_window.append(job)
            count3 += 1
        # 
        # if job.START_TIMESTAMP >= start_datetime and job.START_TIMESTAMP <= end_datetime and job.QUEUED_TIMESTAMP < start_datetime:
        #     count4 +=4

    if slowdownThreshold is True:
        # compute the slowdown threshold for the jobs in the window
        slowdown_values = [compute_slowdown(job) for job in jobs_in_window]
        slowdown_threshold = np.percentile(slowdown_values, 95)

        # remove any jobs that have slowdowns above the threshold
        new_jobs_in_window = []
        for job in jobs_in_window:
            if compute_slowdown(job) < slowdown_threshold:
                new_jobs_in_window.append(job)

        old_jobs_in_window_len = len(jobs_in_window)
        jobs_in_window = new_jobs_in_window

    # get utilization for all jobs that have low enough slowdown, regardless of window
    utilization_jobs = []
    if slowdownThreshold is True:
        for job in jobs:
            if compute_slowdown(job) < slowdown_threshold:
                utilization_jobs.append(job)
    else:
        utilization_jobs = jobs

    slowdowns = []
    turnaround_times = []
    runtimes = []
    slowdown_category = {'narrow_short': [], 'narrow_long': [], 'wide_short': [], 'wide_long': []}
    turnaround_times_category = {'narrow_short': [], 'narrow_long': [], 'wide_short': [], 'wide_long': []}
    category_count = {'narrow_short': 0, 'narrow_long': 0, 'wide_short': 0, 'wide_long': 0}

    # compute metrics for all low SD jobs in the window
    for job in jobs_in_window:
        if job.START_TIMESTAMP <= metrics_start_datetime or job.END_TIMESTAMP >= metrics_end_datetime:
            continue

        slowdown = compute_slowdown(job)
        turnaround_time = compute_turnaround_time(job)
        # turnaround_time = (job.END_TIMESTAMP - job.QUEUED_TIMESTAMP).total_seconds()

        slowdowns.append(slowdown)
        turnaround_times.append(turnaround_time)
        runtimes.append(job.RUNTIME_SECONDS)

        # this code uses runtime instead of walltime for job categorization
        this error is here intentionally, address comment above if trying to use this code

        if job.NODES_USED <= 4096:  # if job is narrow
            if job.RUNTIME_SECONDS <= 120 * 60.0:  # if job is short
                job_category = 'narrow_short'
            else:  # if job is long
                job_category = 'narrow_long'
        else:  # if job is wide
            if job.RUNTIME_SECONDS <= 120 * 60.0:  # if job is short
                job_category = 'wide_short'
            else:  # if job is long
                job_category = 'wide_long'

        slowdown_category[job_category].append(slowdown)
        turnaround_times_category[job_category].append(turnaround_time)
        category_count[job_category] += 1


    all_slowdown_values = [compute_slowdown(job) for job in jobs_in_window]
    all_slowdown_values.sort()
    t = all_slowdown_values[::-1]

    avg_slowdown = sum(slowdowns) / float(len(slowdowns))
    avg_turnaround_time = sum(turnaround_times) / float(len(turnaround_times)) / 60.0
    avg_runtime = sum(runtimes) / float(len(runtimes)) / 60.0
    print('')
    print('system utilization % ' + str(get_utilization_over_window(utilization_jobs, metrics_start_datetime, metrics_end_datetime)))
    # print('system utilization % ' + str(get_utilization_over_window(utilization_jobs, start_datetime+timedelta(days=1), start_datetime+timedelta(days=6))))
    # print('system utilization % ' + str(get_utilization_over_window(jobs, start_datetime+timedelta(days=1), start_datetime+timedelta(days=6))))
    print('avg bounded slowdown ' + str(avg_slowdown))
    print('avg turnaround_times (min) ' + str(avg_turnaround_time))
    print('avg runtime (min) ' + str(avg_runtime))
    print('max slowdown: ' + str(max(slowdowns)))

    print('')
    print(avg_slowdown)
    print(avg_turnaround_time)
    print(len(slowdowns))
    print('')
    for category in ['narrow_short', 'narrow_long', 'wide_short', 'wide_long']:
        print(float(sum(slowdown_category[category])) / len(slowdown_category[category]))
        print(float(sum(turnaround_times_category[category])) / len(turnaround_times_category[category]) / 60.0)
        print(category_count[category])
        print('')

    if slowdownThreshold is True:
        print('slowdown threshold (95%) ' + str(slowdown_threshold))
        print('orig jobs len: ' + str(old_jobs_in_window_len))
        print('new jobs len: ' + str(len(new_jobs_in_window)))

    adjusted_jobs_ids = []
    output_strings = []
    for job in jobs_in_window:
        # if job.START_TIMESTAMP < metrics_start_datetime:
        #     job.QUEUED_TIMESTAMP = metrics_start_datetime
        #     job.START_TIMESTAMP =  metrics_start_datetime
        #     job.END_TIMESTAMP = job.START_TIMESTAMP + timedelta(seconds=job.RUNTIME_SECONDS)

        if job.START_TIMESTAMP < metrics_start_datetime:
            job.QUEUED_TIMESTAMP = job.START_TIMESTAMP
            adjusted_jobs_ids.append(job.COBALT_JOBID)
        
        job.UTILIZATION_AT_QUEUE_TIME = get_utilization_at_time(jobs, job.QUEUED_TIMESTAMP)
        output_strings += make_job_cobalt_log_strings(job)

    with open(output_log_name, 'w') as file:  # Use file to refer to the file object
        for output_string in output_strings:
            file.write(output_string + '\n')

    # print(sorted(adjusted_jobs_ids))

def make_job_cobalt_log_strings(job):
    hours, remainder = divmod(job.WALLTIME_SECONDS, 3600)
    minutes, seconds = divmod(remainder, 60)
    wall_time_time = str('%02d:%02d:%02d' % (hours, minutes, seconds))

    def make_timestamp(tmp_datetime):
        epoch = datetime.utcfromtimestamp(0)
        return (tmp_datetime - epoch).total_seconds()

    output_string1 = job.START_TIMESTAMP.strftime('%m/%d/%Y %H:%M:%S') + ';S;' + str(job.COBALT_JOBID)
    output_string1 += ';queue=default qtime=' + str(make_timestamp(job.QUEUED_TIMESTAMP))
    output_string1 += ' Resource_List.nodect=' + str(int(job.NODES_USED))
    output_string1 += ' Resource_List.walltime=' + wall_time_time
    output_string1 += ' start=' + str(make_timestamp(job.START_TIMESTAMP)) + ' exec_host=' + job.LOCATION
    output_string1 += ' utilization_at_queue_time=' + str(job.UTILIZATION_AT_QUEUE_TIME)
    output_string1 += ' queue_name=' + str(job.QUEUE_NAME)

    output_string2 = job.END_TIMESTAMP.strftime('%m/%d/%Y %H:%M:%S') + ';E;' + str(job.COBALT_JOBID)
    output_string2 += ';queue=default qtime=' + str(make_timestamp(job.QUEUED_TIMESTAMP))
    output_string2 += ' Resource_List.nodect=' + str(int(job.NODES_USED))
    output_string2 += ' Resource_List.walltime=' + wall_time_time + ' start=' + str(make_timestamp(job.START_TIMESTAMP))
    output_string2 += ' end=' + str(make_timestamp(job.END_TIMESTAMP)) + ' exec_host=' + job.LOCATION
    output_string2 += ' runtime=' + str(job.RUNTIME_SECONDS) + ' hold=0 overhead=0'
    output_string2 += ' utilization_at_queue_time=' + str(job.UTILIZATION_AT_QUEUE_TIME)
    output_string2 += ' queue_name=' + str(job.QUEUE_NAME)


    # output_string1 = job.START_TIMESTAMP.strftime('%m/%d/%Y %H:%M:%S') + ';S;' + str(job.COBALT_JOBID) + ';queue=default qtime=' + str(job.QUEUED_TIMESTAMP.timestamp())
    # output_string1 += ' Resource_List.nodect=' + str(int(job.NODES_USED)) + ' Resource_List.walltime=' + wall_time_time + ' start=' + str(job.START_TIMESTAMP.timestamp()) + ' exec_host=' + job.LOCATION
    # output_string1 += ' utilization_at_queue_time=' + str(job.UTILIZATION_AT_QUEUE_TIME)
    #
    # output_string2 = job.END_TIMESTAMP.strftime('%m/%d/%Y %H:%M:%S') + ';E;' + str(job.COBALT_JOBID) + ';queue=default qtime=' + str(job.QUEUED_TIMESTAMP.timestamp())
    # output_string2 += ' Resource_List.nodect=' + str(int(job.NODES_USED)) + ' Resource_List.walltime=' + wall_time_time + ' start=' + str(job.START_TIMESTAMP.timestamp())
    # output_string2 += ' end=' + str(job.END_TIMESTAMP.timestamp()) + ' exec_host=' + job.LOCATION + ' runtime=' + str(job.RUNTIME_SECONDS) + ' hold=0 overhead=0'
    # output_string2 += ' utilization_at_queue_time=' + str(job.UTILIZATION_AT_QUEUE_TIME)

    # print(output_string1)
    # print(output_string2)
    return [output_string1, output_string2]

# 11/07/2014 22:58:40;S;359758;queue=default qtime=1415414459.0 Resource_List.nodect=4096 Resource_List.walltime=00:20:00 start=1415422720.9 exec_host=MIR-08800-3BFF1-3-4096
# 11/07/2014 23:02:10;E;359758;queue=default qtime=1415414459.0 Resource_List.nodect=4096 Resource_List.walltime=00:20:00 start=1415422720.9 end=1415422930.000000 exec_host=MIR-08800-3BFF1-3-4096 runtime=209.1 hold=0 overhead=0



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

def test_weird_utilization(jobs, start_datetime):
    # val = get_utilization_over_window(jobs, start_datetime, start_datetime + timedelta(days=1))

    start_time = datetime(2018, 6, 9)
    end_time = start_time + timedelta(days=1)
    total_core_hours = 0.0
    for job in jobs:
        if job.START_TIMESTAMP >= start_time and job.END_TIMESTAMP <= end_time:
            total_core_hours += (job.END_TIMESTAMP-job.START_TIMESTAMP).total_seconds() * job.NODES_USED
        elif job.START_TIMESTAMP < start_time and job.END_TIMESTAMP > start_time and job.END_TIMESTAMP <= end_time:
            total_core_hours += (job.END_TIMESTAMP - start_time).total_seconds() * job.NODES_USED
        elif job.START_TIMESTAMP >= start_time and job.START_TIMESTAMP < end_time and job.END_TIMESTAMP > end_time:
            total_core_hours += (end_time - job.START_TIMESTAMP).total_seconds() * job.NODES_USED
        elif job.START_TIMESTAMP < start_time and job.END_TIMESTAMP > end_time:
            total_core_hours += (end_time-start_time).total_seconds() * job.NODES_USED
    total_possible_core_hours = (end_time - start_time).total_seconds() * total_nodes
    val = total_core_hours / ((end_time - start_time).total_seconds() * total_nodes)


    start_time += timedelta(hours=7)

    print(val)
    print('intervals)')
    increment = timedelta(minutes=15)
    while start_time < end_time:
        print(str(start_time) + ' - ' + str(get_utilization_over_window(jobs, start_time, start_time + increment)))
        start_time += increment
    exit()
    
    

def test_over_100_utilization(jobs, start_datetime=None):
    # val = get_utilization_over_window(jobs, start_datetime, start_datetime + timedelta(days=1))
    
    time_stamps_list = [job.START_TIMESTAMP for job in jobs]
    time_stamps_list.sort()
    # time_stamps_list = time_stamps_list + [job.END_TIMESTAMP for job in jobs]

    for current_time in time_stamps_list:
        current_util, current_jobs = get_utilization_at_time_with_jobs(jobs, current_time)
        if current_util > 1.0:
            jobs_queue_dict = {}
            for job in current_jobs:
                if job.QUEUE_NAME in jobs_queue_dict:
                    jobs_queue_dict[job.QUEUE_NAME] = (jobs_queue_dict[job.QUEUE_NAME][0] + 1,
                                                           jobs_queue_dict[job.QUEUE_NAME][1] + job.NODES_USED)
                else:
                    jobs_queue_dict[job.QUEUE_NAME] = (1, job.NODES_USED)
            extra_nodes = (current_util-1.0) * total_nodes
            nodes_free_soon = 0
            for job in jobs:
                if job.END_TIMESTAMP >= current_time and job.END_TIMESTAMP <= current_time + timedelta(minutes=10):
                    nodes_free_soon += job.NODES_USED

            print(str(current_time) + ' - ' + str(current_util) + '% - ' + str(extra_nodes) + ' extra nodes - '
                  + str(nodes_free_soon) + ' nodes free soon - ' + str(jobs_queue_dict))
    exit()


def get_utilization_over_window(jobs, start_time, end_time):
    total_core_hours = 0.0
    jobs_in_window = []
    for job in jobs:
        if job.COBALT_JOBID == 1557740:
            pass
        if job.START_TIMESTAMP >= start_time and job.END_TIMESTAMP <= end_time:
            total_core_hours += (job.END_TIMESTAMP-job.START_TIMESTAMP).total_seconds() * job.NODES_USED
            jobs_in_window.append(job)
        elif job.START_TIMESTAMP < start_time and job.END_TIMESTAMP > start_time and job.END_TIMESTAMP <= end_time:
            total_core_hours += (job.END_TIMESTAMP - start_time).total_seconds() * job.NODES_USED
            jobs_in_window.append(job)
        elif job.START_TIMESTAMP >= start_time and job.START_TIMESTAMP < end_time and job.END_TIMESTAMP > end_time:
            total_core_hours += (end_time - job.START_TIMESTAMP).total_seconds() * job.NODES_USED
            jobs_in_window.append(job)
        elif job.START_TIMESTAMP < start_time and job.END_TIMESTAMP > end_time:
            total_core_hours += (end_time-start_time).total_seconds() * job.NODES_USED
            jobs_in_window.append(job)
    # for job in jobs_in_window:
        # print(str(job.COBALT_JOBID) + ' - ' + str(job.START_TIMESTAMP) + ' - ' + str(job.END_TIMESTAMP) + ' - ' + str(job.NODES_USED))
    # print(sum([job.NODES_USED for job in jobs_in_window]))
    total_possible_core_hours = (end_time - start_time).total_seconds() * total_nodes
    # print(get_utilization_at_time(jobs, start_time))
    return total_core_hours / ((end_time - start_time).total_seconds() * total_nodes)


def get_utilization_at_time(jobs, current_time):
    total_nodes_used = 0.0
    for job in jobs:
        if job.START_TIMESTAMP <= current_time and job.END_TIMESTAMP >= current_time:
            total_nodes_used += job.NODES_USED
    return float(total_nodes_used) / total_nodes


def get_utilization_at_time_with_jobs(jobs, current_time):
    total_nodes_used = 0.0
    current_jobs = []
    for job in jobs:
        if job.START_TIMESTAMP <= current_time and job.END_TIMESTAMP >= current_time:
            total_nodes_used += job.NODES_USED
            current_jobs.append(job)
    return (float(total_nodes_used) / total_nodes, current_jobs)


def parse_jobs():
    input_file = open(csv_file_name)
    input_file_reader = csv.reader(input_file)

    jobs = []

    for row in input_file_reader:
        if row[0] == 'JOB_NAME':
            continue
        new_job = job(row)
        jobs.append(new_job)

    input_file.close()
    return jobs



class job:
    def __init__(self, entry):
        idx = 0
        self.JOB_NAME = entry[idx]
        idx += 1
        self.COBALT_JOBID = int(entry[idx])
        idx += 1
        self.MACHINE_NAME = entry[idx]
        idx += 1
        try:
            self.QUEUED_TIMESTAMP = datetime.strptime(entry[idx], '%Y-%m-%d %H:%M:%S')
        except:
            self.QUEUED_TIMESTAMP = datetime.strptime(entry[idx], '%Y-%m-%d %H:%M:%S.%f')
        idx += 1
        self.QUEUED_DATE_ID = int(entry[idx])
        idx += 1
        try:
            self.START_TIMESTAMP = datetime.strptime(entry[idx], '%Y-%m-%d %H:%M:%S')
        except:
            self.START_TIMESTAMP = datetime.strptime(entry[idx], '%Y-%m-%d %H:%M:%S.%f')
        idx += 1
        self.START_DATE_ID = int(entry[idx])
        idx += 1
        try:
            self.END_TIMESTAMP = datetime.strptime(entry[idx], '%Y-%m-%d %H:%M:%S')
        except:
            self.END_TIMESTAMP = datetime.strptime(entry[idx], '%Y-%m-%d %H:%M:%S.%f')
        idx += 1
        self.END_DATE_ID = int(entry[idx])
        idx += 1
        self.USERNAME_GENID = entry[idx]
        idx += 1
        self.PROJECT_NAME_GENID = entry[idx]
        idx += 1
        self.QUEUE_NAME = entry[idx]
        idx += 1
        self.WALLTIME_SECONDS = float(entry[idx])
        idx += 1
        self.RUNTIME_SECONDS = float(entry[idx])
        idx += 1
        self.NODES_USED = float(entry[idx])
        idx += 1
        self.NODES_REQUESTED = float(entry[idx])
        idx += 1
        self.CORES_USED = float(entry[idx])
        idx += 1
        self.CORES_REQUESTED = float(entry[idx])
        idx += 1
        self.LOCATION = entry[idx]
        idx += 1
        self.EXIT_STATUS = int(entry[idx])
        idx += 1
        self.ELIGIBLE_WAIT_SECONDS = int(entry[idx])
        idx += 1
        self.ELIGIBLE_WAIT_FACTOR = int(entry[idx])
        idx += 1
        self.QUEUED_WAIT_SECONDS = int(entry[idx])
        idx += 1
        self.QUEUED_WAIT_FACTOR = int(entry[idx])
        idx += 1
        self.REQUESTED_CORE_HOURS = float(entry[idx])
        idx += 1
        self.USED_CORE_HOURS = float(entry[idx])
        idx += 1
        self.CAPABILITY_USAGE_CORE_HOURS = float(entry[idx])
        idx += 1
        self.NONCAPABILITY_USAGE_CORE_HOURS = float(entry[idx])
        idx += 1
        self.BUCKETS3_A_USAGE_CORE_HOURS = float(entry[idx])
        idx += 1
        self.BUCKETS3_B_USAGE_CORE_HOURS = float(entry[idx])
        idx += 1
        self.BUCKETS3_C_USAGE_CORE_HOURS = float(entry[idx])
        idx += 1
        self.MACHINE_PARTITION = entry[idx]
        idx += 1
        self.EXIT_CODE = int(entry[idx])
        idx += 1
        self.MODE = entry[idx]
        idx += 1
        self.RESID = int(entry[idx])
        idx += 1
        self.DATA_LOAD_STATUS = entry[idx]
        idx += 1
        self.CAPABILITY = entry[idx]
        idx += 1
        self.SIZE_BUCKETS3 = entry[idx]
        idx += 1
        self.PERCENTILE = entry[idx]
        idx += 1
        self.NUM_TASKS_SUBBLOCK = int(entry[idx])
        idx += 1
        self.NUM_TASKS_CONSECUTIVE = int(entry[idx])
        idx += 1
        self.NUM_TASKS_MULTILOCATION = int(entry[idx])
        idx += 1
        self.NUM_TASKS_SINGLE = int(entry[idx])
        idx += 1
        self.COBALT_NUM_TASKS = int(entry[idx])
        idx += 1
        self.IS_SINGLE = int(entry[idx])
        idx += 1
        self.IS_CONSECUTIVE = int(entry[idx])
        idx += 1
        self.IS_MULTILOCATION = int(entry[idx])
        idx += 1
        self.IS_SUBBLOCK = int(entry[idx])
        idx += 1
        self.IS_SUBBLOCK_ONLY = int(entry[idx])
        idx += 1
        self.IS_MULTILOCATION_ONLY = int(entry[idx])
        idx += 1
        self.IS_MULTILOCATION_SUBBLOCK = int(entry[idx])
        idx += 1
        self.IS_CONSECUTIVE_ONLY = int(entry[idx])
        idx += 1
        self.IS_SINGLE_ONLY = int(entry[idx])
        idx += 1
        self.IS_NO_TASKS = int(entry[idx])
        idx += 1
        self.IS_OTHER = int(entry[idx])
        idx += 1
        self.OVERBURN_CORE_HOURS = float(entry[idx])
        idx += 1
        self.IS_OVERBURN = int(entry[idx])
        idx += 1

        self.UTILIZATION_AT_QUEUE_TIME = -1.0


if __name__== "__main__":
  main()