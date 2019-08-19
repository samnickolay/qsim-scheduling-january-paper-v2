
import argparse
import datetime
import os
import pickle
import csv

# python pickle-parsing-script.py
# python cobalt-master/cobalt-support-scripts/pickle-parsing-script.py -O job_metric_spreadsheets -P test.pickle

total_nodes = 49152


parser = argparse.ArgumentParser(description='Parse the output pickle files from Cobalt simulations')

start_time = datetime.datetime.now()
print('start time: ' + str(start_time))

class jobInterval():
    def __init__(self, type, start_time, end_time):
        self.type = type
        self.start_time = start_time
        self.end_time = end_time

        assert self.type in ['queued', 'running', 'checkpointing', 'wasted', 'waiting', 'restarting']

    def __str__(self):
        return '(jobInterval: ' + self.type + ', ' + str(self.start_time) + ', ' + str(self.end_time) + ', ' + str(self.end_time-self.start_time) + ')'
#

# add the arguments to the parser
parser.add_argument('--pickle_files', '-P', dest='input_pickle_files', type=str, nargs='+',
                    help='the pickle files or folders containing pickle files to parse', required=True)

parser.add_argument('--output_folder', '-O', dest='input_output_folder', type=str,
                    help='the folders to output files in to', required=True)

parser.add_argument('--test', '-t', default=False, action='store_true',
                    help="if test flag is passed then the script will not actually run the cobalt simulations")


def main():
    # parse the arguments
    args = parser.parse_args()

    input_pickle_files = args.input_pickle_files
    output_folder = args.input_output_folder

    if output_folder[-1] != '/':
        output_folder += '/'

    if not os.path.exists(output_folder):
        print('Output folder provided does not exist - creating output folder')
        print(output_folder)
        os.mkdir(output_folder)

    valid_input = False
    for input_pickle_file in input_pickle_files:

        if not os.path.exists(input_pickle_file):
            print("Invalid file/folder given for input_pickle_files - file doesn't exist - ", input_pickle_file)
        else:
            if os.path.isdir(input_pickle_file):
                for sub_filename in os.listdir(input_pickle_file):
                    if sub_filename.split('.')[-1] == 'pickle':
                        return_value = parse_pickle_file(os.path.join(input_pickle_file, sub_filename), output_folder)
                        if return_value is True:
                            return_value = True
            else:
                if input_pickle_file.split('.')[-1] == 'pickle':
                    return_value = parse_pickle_file(input_pickle_file, output_folder)
                    if return_value is True:
                        return_value = True
    if return_value is False:
        print("Error - No valid pickle files provided! - ")
        print(input_pickle_files)


def make_job_metrics_spreadsheet(jobs_metrics, spreadsheet_filename):
    headers = ['queued_time', 'log_start_time', 'log_end_time', 'log_run_time', 'log_slowdown', 'log_turnaround_time', 
               'log_queue_time', 'log_utilization_at_queue_time', 'log_queue_name', 'nodes', 'wall_time', 'run_time', 'slowdown', 'turnaround_time',
               'initial_queue_time', 'utilization_at_queue_time', 'trimmed', 'job_type', 'job_category', 'location',
               'start_times', 'end_times', 'queue_time', 'wait_time', 'checkpoint_time', 'restart_time', 'waste_time',
               'queue_count', 'wait_count', 'checkpoint_count', 'restart_count', 'waste_count']
    # extra_headers = ['first_start_time', 'last_end_time']
    # all_headers = headers + extra_headers
    
    with open(spreadsheet_filename, mode='w') as spreadsheet_file:
        employee_writer = csv.writer(spreadsheet_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        
        employee_writer.writerow(['jobid'] + headers)
    
        for jobid, job in jobs_metrics.iteritems():
            job['start_times'] = job['start_times'][0]
            job['end_times'] = job['end_times'][-1]

            datetime_headers = ['queued_time', 'log_start_time', 'log_end_time', 'start_times', 'end_times']
            for datetime_header in datetime_headers:
                job[datetime_header] = datetime.datetime.utcfromtimestamp(job[datetime_header]).strftime("%m/%d/%Y %H:%M:%S")

            current_values = [jobid] + [job[header] for header in headers]



            employee_writer.writerow(current_values)


def print_utilization_records(utilization_records):
    for utilization_record in utilization_records:
        print(datetime.datetime.utcfromtimestamp(utilization_record[1]).strftime("%m/%d/%Y %H:%M:%S") + ', '+ str(utilization_record[2]))
        # print(utilization_record)
        
def get_datetime_str(date_time):
    return datetime.datetime.utcfromtimestamp(date_time).strftime("%m/%d/%Y %H:%M:%S")

def print_jobs_intervals(jobs_metrics):
    jobs_interval_strs = []
    for jobid, job_metrics in jobs_metrics.iteritems():
        
        for job_interval in job_metrics['intervals']:
            str_header = str(jobid) + ' - ' + str(jobs_metrics[jobid]['nodes'])
            str1 = str_header + ' - started ' + job_interval[0] + ' at ' + get_datetime_str(job_interval[1])
            str2 = str_header + ' - finished ' + job_interval[0] + ' at ' + get_datetime_str(job_interval[2])
            jobs_interval_strs.append((job_interval[1], jobid, str1))
            jobs_interval_strs.append((job_interval[2], jobid, str2))

    jobs_interval_strs.sort()
    current_nodes = 0
    for jobs_interval_str in jobs_interval_strs:
        if 'started running' in jobs_interval_str[2]:
            current_nodes += jobs_metrics[jobs_interval_str[1]]['nodes']
        elif 'finished running' in jobs_interval_str[2]:
            current_nodes -= jobs_metrics[jobs_interval_str[1]]['nodes']
        nodes_str = ' - Nodes: ' + str(current_nodes) + '/'+ str(total_nodes) + ' (' + str(float(current_nodes) / total_nodes) + ')'
        print(jobs_interval_str[2] + nodes_str)
    # return [jobs_interval_str[1] for jobs_interval_str in jobs_interval_strs]


def output_utilization_records(utilization_records):
    old_time = 0.0
    for utilization_record in utilization_records:
        print(utilization_record)

        # if utilization_record[0] > old_time + 60 * 5.0:
        #     print(utilization_record)
        #     old_time = utilization_record[0]


def look_for_blocking_low_priority_queue_jobs_in_utilization_records(utilization_records):
    print('-----')
    print('printing utilization intervals with blocking low priority queue jobs')
    print('-----')
    low_priority_blocking_record_count = 0
    high_prioirty_record_count = 0
    extra_nodes_run = 0
    extra_low_priority_core_hours = 0
    for utilization_record in utilization_records:
        print(utilization_record)

        if len(utilization_record) > 3:

            high_prioirity_queue_jobs = utilization_record[4]
            low_prioirity_queue_jobs = utilization_record[3]
            current_utilization = utilization_record[2]
            end_time = utilization_record[1]
            start_time = utilization_record[0]

            current_free_nodes = (1.0 - current_utilization) * total_nodes
            if len(high_prioirity_queue_jobs) > 0:
                high_prioirty_record_count += 1

                if current_free_nodes > 0:
                    low_priority_queue_jobs_ints = [int(val) for val in low_prioirity_queue_jobs]
                    for low_prioirity_queue_job in low_priority_queue_jobs_ints:

                        if low_prioirity_queue_job <= current_free_nodes:
                            current_extra_nodes_run = min(current_free_nodes, sum(low_priority_queue_jobs_ints))
                            low_priority_blocking_record_count += 1
                            extra_nodes_run += current_extra_nodes_run
                            current_extra_core_hours = current_extra_nodes_run * (end_time-start_time) / 3600.0
                            extra_low_priority_core_hours += current_extra_core_hours
                            print('-- extra nodes run: ' + str(extra_nodes_run))
                            print('-- extra core hours: ' + str(current_extra_core_hours))
                            break


    print('-----')
    print('total intervals: ' + str(len(utilization_records)))
    print('number of intervals with waiitng high priority queue jobs: ' + str(high_prioirty_record_count))
    print('number of intervals with blocking low priority queue jobs: ' + str(low_priority_blocking_record_count))
    print('numbers of potential extra nodes run: ' + str(extra_nodes_run))
    print('numbers of potential extra core hours: ' + str(extra_low_priority_core_hours))
    total_core_hours = (utilization_records[-1][1] - utilization_records[0][0]) / 3600.0 * total_nodes
    print('numbers of potential extra core hours %: ' + str(extra_low_priority_core_hours/total_core_hours * 100.0))

    print('-----')


def parse_pickle_file(pickle_filename, output_folder):
    try:
        pickle_data = pickle.load(open(pickle_filename, "rb"))
    except:
        print('Error parsing pickle file - ', pickle_filename)
        return False

    experiment_metrics = pickle_data['experiment_metrics']
    jobs_metrics = pickle_data['jobs_metrics']
    trimmed_utilization_records = pickle_data['trimmed_utilization_records']
    utilization_records = pickle_data['utilization_records']

    pickle_filename_base = os.path.basename(pickle_filename)
    spreadsheet_filename = os.path.join(output_folder, os.path.splitext(pickle_filename_base)[0]) + '.csv'
    make_job_metrics_spreadsheet(jobs_metrics, spreadsheet_filename)
    
    # utilization_records_filename = os.path.join(output_folder, os.path.splitext(pickle_filename_base)[0]) + '.txt'
    # output_utilization_records(trimmed_utilization_records)

    look_for_blocking_low_priority_queue_jobs_in_utilization_records(trimmed_utilization_records)

    # print_utilization_records(utilization_records)
    # print_jobs_intervals(jobs_metrics)

    return True


if __name__== "__main__":
    start_time = datetime.datetime.now()
    print('start time: ' + str(start_time))

    main()

    print('\ndone!\n')

    end_time = datetime.datetime.now()
    print('end time: ' + str(end_time))
    print('run time: ' + str(end_time - start_time))