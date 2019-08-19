# job class
from datetime import datetime, timedelta, time
import csv
import numpy as np

# csv_file_name = 'ANL-ALCF-DJC-MIRA_20170101_20171231.csv'
csv_file_name = 'CEA_curie_log_original.csv'


input_file_name = 'cea_curie_cobalt_ready_no_rtj.log'
output_file_name = 'cea_curie_cobalt_ready_no_rtj.log'


cobalt_partition_sizes = [512, 1024, 2048, 4096, 8192, 12288, 16384, 24576, 32768, 49152]

number_of_rtjs_to_add = 10
start_time = datetime(year=1971, month=2, day=17)
end_time = datetime(year=1971, month=2, day=17)

runtimes = [ 93, 110, 90, 80, 86, 68, 448, 95, 65, 103, 98, 91, 362, 387, 396, 97, 76, 72, 91, 407, 406, 424, 83, 86,
             77, 88, 391, 381, 419, 78, 79, 78, 374, 409, 371, 73, 81, 91, 372, 381, 368, 82, 79, 361, 367, 407, 77,
             80, 84, 81, 395, 377, 388, 89, 344, 366, 382, 77, 82, 86, 376, 372, 459, 82, 369, 395, 360, 80, 77, 411,
             390, 427, 75, 87, 426, 392, 381, 82, 350, 377, 430, 93, 388, 373, 402, 83, 75, 70, 79, 406, 371, 415, 89,
             70, 85, 431, 393, 356, 81, 86, 372, 350, 373, 80, 85, 374, 388, 394, 72, 84, 358, 352, 439, 75, 71, 406,
             387, 376, 79, 85, 90, 400, 443, 396, 78, 85, 366, 395, 373, 90, 78, 92, 321, 410, 414, 94, 85, 79, 77,
             383, 376, 400, 91, 88, 335, 429, 426, 73, 72, 73, 336, 390, 410, 83, 365, 416, 378, 99, 82, 78, 353,
             366, 404, 73, 78, 384, 402, 394, 83, 84, 76, 397, 436, 405, 93, 74, 77, 88, 374, 411, 370, 71, 81, 95,
             83, 401, 377, 378, 93, 83, 88, 331, 391, 395, 80, 84, 366, 434, 374, 91, 89, 365, 387, 375, 87, 344,
             382, 374, 89, 84, 327, 415, 403, 73, 88, 385, 393, 400, 82, 85, 354, 398, 400, 78, 72, 90, 324, 367,
             355, 89, 345, 373, 362, 78, 75, 80, 399, 126, 161, 123, 122, 112, 130, 151, 131, 141, 122, 128, 138, 138,
             116, 102, 107, 125, 134, 149, 86, 129, 128, 116, 99, 125, 114, 114, 93, 127, 132, 129, 92, 138, 101, 151,
             81, 125, 142, 138, 83, 142, 135, 131, 90, 132, 152, 145, 98, 130, 120, 128, 86, 132, 122, 140, 93, 112,
             124, 121, 105, 119, 147, 138, 93, 126, 117, 123, 84, 136, 125, 120, 98, 113, 124, 136, 97, 121, 136, 118,
             84, 137, 120, 132, 95, 143, 128, 147, 85, 133, 128, 127, 91, 129, 133, 119, 83, 108, 141, 134, 88, 116,
             151, 134, 106, 155, 135, 94, 130, 126, 136, 95, 121, 130, 127, 111, 123, 137, 100, 100, 128, 123, 87,
             126, 131, 130, 89, 133, 124, 148, 112, 117, 136, 141, 90, 132, 133, 132, 89, 131, 152, 143, 102, 59, 60,
             60, 79, 65, 60, 59, 74, 96, 60, 60, 60, 72, 114, 135, 109, 133, 140, 114, 102, 123, 121, 134, 81, 90, 126,
             117, 119, 89, 97, 138, 135, 80, 115, 125, 127, 90, 168, 119, 144, 80, 91, 125, 129, 105, 78, 88, 77, 97,
             127, 143, 110, 90, 74, 79, 122, 150, 133, 106, 116, 122, 129, 95, 125, 123, 135, 91, 74, 87, 84, 138, 145,
             126, 90, 138, 126, 145, 83, 90, 125, 132, 145, 81, 84, 78, 88, 118, 142, 164, 89, 112, 60, 89, 61, 62,
             132, 104, 108, 138, 90, 172, 150, 168, 103, 97, 128, 221, 86, 147, 104, 179, 197, 102, 131, 155, 159, 151,
             105, 128, 155, 135, 93, 141, 158, 131, 94, 150, 129, 191, 160, 113, 148, 143, 153, 127, 148, 162, 139,
             118, 149, 174, 152, 133, 155, 141, 121, 131, 131, 177, 145, 106, 152, 142, 148, 182, 119, 114, 149, 135,
             135, 131, 135, 136, 163, 127, 127, 159, 137, 174, 132, 116, 148, 154, 168, 152, 80, 89, 91, 84, 97, 77,
             91, 106, 111, 99, 85, 94, 117, 103, 65, 99, 100, 82, 105, 112, 90, 111, 106, 80, 112, 86, 136, 93, 100,
             90, 97, 113, 112, 114, 97, 113, 98, 96, 94, 103, 105, 122, 84, 109, 94, 100, 86, 130, 81, 129, 96, 82,
             110, 117, 99, 94, 645, 705, 530, 557, 635, 622, 531, 608, 526, 569, 617, 558, 566, 592, 547, 515, 548,
             662, 673, 583, 554, 519, 503, 490, 506, 491, 492, 517, 469, 523, 475, 451, 489, 491, 495, 526, 505, 517,
             463, 518, 482, 510, 475, 490, 521, 484, 515, 502, 475, 535, 474, 485, 513, 519, 518, 505, 518, 463, 503,
             505, 507, 483, 547, 478, 533, 491, 542, 466, 536, 488, 504, 524, 531, 490, 532, 541, 506, 508, 509, 510,
             549, 534, 501, 525, 487, 522, 533, 496, 565, 494, 530, 543, 530, 518, 501, 507, 500, 554, 463, 534, 517,
             519, 514, 526, 536, 558, 515, 531, 541, 525, 496, 518, 545, 526, 492, 529, 518, 533, 486, 552, 540, 531,
             566, 515, 514, 506, 512, 502, 565, 488, 543, 475, 534, 510, 522, 517, 551, 477, 502, 542, 505, 502, 531,
             566, 556, 494, 543, 101, 108, 102, 106, 99, 104, 98, 92, 92, 401, 120, 532, 106, 529, 589, 514, 549, 566,
             107, 606, 537, 522, 110, 105, 101, 109, 584, 92, 555, 419, 389, 466, 493, 460, 436, 437, 411, 433, 477,
             432, 86, 93, 89, 91, 97, 97, 96, 416, 414, 497, 448, 422, 436, 405, 459, 439, 469, 432, 93, 92, 100, 90,
             95, 97, 90, 92, 481, 432, 443, 428, 424, 449, 427, 442, 418, 449, 465, 402, 419, 442, 457, 455, 445, 411,
             394, 407, 372, 424, 452, 401, 453, 393, 511, 437, 404, 472, 93, 84, 87, 86, 93, 83, 89, 86, 92, 88, 95,
             89, 366, 416, 461, 387, 448, 466, 451, 473, 429, 405, 468, 440, 447, 453, 440, 416, 462, 432, 420, 445,
             436, 468, 389, 459, 420, 454, 430, 460, 467, 450, 87, 87, 92, 82, 88, 96, 92, 94, 88, 84, 84, 87, 91, 86,
             86, 84, 87, 93, 457, 405, 417, 410, 442, 418, 389, 419, 433, 392, 462, 435, 476, 439, 462, 462, 442, 440,
             453, 433, 448, 441, 474, 427, 95, 85, 77, 85, 89, 85, 82, 92, 83, 99, 94, 106, 93, 101, 92, 425, 395, 484,
             480, 434, 450, 394, 428, 419, 409, 388, 434, 446, 383, 425, 405, 458, 413, 475, 438, 423, 419, 443, 478,
             381, 450, 416, 85, 89, 90, 90, 89, 92, 88, 97, 92, 97, 98, 85, 88, 91, 87, 93, 92]


def main():
    jobs = parse_jobs()


    print('list utilization by weeks')
    start_datetime = jobs[0].START_TIMESTAMP + timedelta(days=4)
    end_datetime = jobs[-1].START_TIMESTAMP
    one_week = timedelta(days=7)
    two_week = timedelta(days=14)

    list_utilization_by_weeks(jobs, start_datetime, end_datetime, one_week)

    ##################

    print('making cobalt logs for CEA Curie logs')

    start_datetime1 = datetime(year=1971, month=2, day=17)
    end_datetime1 = start_datetime1 + timedelta(days=14)

    metrics_start_datetime1 = start_datetime1 + timedelta(days=2)
    metrics_end_datetime1 = metrics_start_datetime1 + timedelta(days=7)

    output_log_name = 'cea_curie_cobalt_ready_no_rtj.log'
    make_cobalt_logs(jobs, start_datetime1, end_datetime1, metrics_start_datetime1, metrics_end_datetime1,
                     output_log_name, slowdownThreshold=False)

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

        # job_time = job.RUNTIME_SECONDS  # this code uses runtime instead of walltime for job categorization
        job_time = job.WALLTIME_SECONDS

        if job.NODES_USED <= 4096:  # if job is narrow
            if job_time <= 120 * 60.0:  # if job is short
                job_category = 'narrow_short'
            else:  # if job is long
                job_category = 'narrow_long'
        else:  # if job is wide
            if job_time <= 120 * 60.0:  # if job is short
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

    for idx, row in enumerate(input_file_reader):
        if idx == 0 or row[0] == 'JOB_NAME':
            continue
        new_job = job(row)

        if new_job.RUNTIME_SECONDS <=0:
            continue
        elif new_job.curie_nodes <= 0:
            continue
        elif new_job.curie_nodes < 256:
        # elif new_job.mira_nodes_unrounded < 135:
            continue

        jobs.append(new_job)

    input_file.close()
    return jobs



class job:
    def __init__(self, id, queue_time, start_time, end_time, walltime, nodes):
        # job, submit, wait, runtime, proc alloc, cpu used, mem used, proc req, user est, mem req, status, uid, gid, exe num, q num, partition, prev job, think time
        #   0,      1,     2,      3,          4,        5,         6,       7,        8,       9,     10,  11,  12,      13,    14,        15,       16,    17

        self.COBALT_JOBID = id
        self.QUEUED_TIMESTAMP = queue_time
        self.START_TIMESTAMP = start_time
        self.END_TIMESTAMP = end_time
        self.ELIGIBLE_WAIT_SECONDS = (start_time - queue_time).total_seconds()
        self.WALLTIME_SECONDS = walltime
        self.RUNTIME_SECONDS = (end_time - start_time).total_seconds()
        self.NODES_USED = nodes
        self.LOCATION = "default"
        self.QUEUE_NAME = "default"
        self.UTILIZATION_AT_QUEUE_TIME = -1.0


if __name__== "__main__":
  main()