'''
ssh -t samnickolay@login.jlse.anl.gov "cd /gpfs/jlse-fs0/users/samnickolay/ ; bash"

cd /gpfs/jlse-fs0/users/samnickolay/

rm -rf /gpfs/jlse-fs0/users/samnickolay/*

git clone https://github.com/samnickolay/qsim-scheduling.git /gpfs/jlse-fs0/users/samnickolay/cobalt-master


# testing for this version being the code used with the january paper
git clone https://github.com/samnickolay/qsim-scheduling-january-paper.git /gpfs/jlse-fs0/users/samnickolay/cobalt-master
chmod +x /gpfs/jlse-fs0/users/samnickolay/cobalt-master/cobalt-support-scripts/qsub_script_v1.sh
mkdir /gpfs/jlse-fs0/users/samnickolay/cobalt-master/log


Week 1 command
qsub -q gomez -t 6:00:00 -n 1 /gpfs/jlse-fs0/users/samnickolay/cobalt-master/cobalt-support-scripts/qsub_script_v1.sh "python /gpfs/jlse-fs0/users/samnickolay/cobalt-master/cobalt-support-scripts/cobalt-automating-script-parallel.py --rt_percents 5 10 15 20 --trials 5 --rt_job_categories all --checkpoint_heuristics baseline v2_sam_v1 v2p_app_sam_v1 --runtime_estimators walltime --checkp_overhead_percents 5 10 20 --log_file_name wk1.log --results_window_start 2018/01/21 --results_window_length 7"

CEA Curie command
qsub -q gomez -t 6:00:00 -n 1 /gpfs/jlse-fs0/users/samnickolay/cobalt-master/cobalt-support-scripts/qsub_script_v1.sh "python /gpfs/jlse-fs0/users/samnickolay/cobalt-master/cobalt-support-scripts/cobalt-automating-script-parallel.py --rt_percents 5 10 15 20 --trials 5 --rt_job_categories all --checkpoint_heuristics baseline v2_sam_v1 v2p_app_sam_v1 --runtime_estimators walltime --checkp_overhead_percents 5 10 20 --log_file_name cea_curie_cobalt_ready_no_rtj.log --results_window_start 1971/02/20 --results_window_length 7"


results_directory="results_8-15-19_cea_curie_v1"

results_directory="results_8-19-19_wk1_v2_test"
mkdir -p $results_directory
scp -r samnickolay@login.jlse.anl.gov:/gpfs/jlse-fs0/users/samnickolay/cobalt-master/src/components/results/* /home/samnickolay/$results_directory/



-------------------------

useful commands:
cat automating-script-output-wk1.log - this outputs the current status of the job running (assumes you're in your home directory - /gpfs/jlse-fs0/users/samnickolay)

qsub - submits a job
qstat - lists all currently running jobs
qdel - deletes a currently running job

'''




'''
### transfer the simulator to the remote file system ###

rm -rf /home/samnickolay/cobalt-master/

git clone https://github.com/samnickolay/qsim-scheduling.git cobalt-master

### transfer the result files back ###

cp /home/samnickolay/automating-script-output-wk*.log /home/samnickolay/cobalt-master/src/components/results/

results_directory="results_2-5-19_v1"
mkdir -p $results_directory
scp -r samnickolay@login.jlse.anl.gov:/home/samnickolay/cobalt-master/src/components/results/* /home/samnickolay/$results_directory/

scp -r samnickolay@login.jlse.anl.gov:/home/samnickolay/automating-script-output-wk*.log /home/samnickolay/$results_directory/automating-script-output-wk1.log


# Old Commands
# scp -r samnickolay@login.jlse.anl.gov:/home/samnickolay/cobalt-master/src/components/slowdown_results /home/samnickolay/$results_directory/slowdown_results/
# scp -r samnickolay@login.jlse.anl.gov:/home/samnickolay/cobalt-master/src/components/simulation_output /home/samnickolay/$results_directory/simulation_output/
# scp -r samnickolay@login.jlse.anl.gov:/home/samnickolay/cobalt-master/src/components/simulation_pickled_data /home/samnickolay/$results_directory/simulation_pickled_data/
#
# scp -r samnickolay@login.jlse.anl.gov:/home/samnickolay/automating-script-output-wk1.log /home/samnickolay/$results_directory/automating-script-output-wk1.log
# scp -r samnickolay@login.jlse.anl.gov:/home/samnickolay/automating-script-output-wk2.log /home/samnickolay/$results_directory/automating-script-output-wk2.log
# scp -r samnickolay@login.jlse.anl.gov:/home/samnickolay/automating-script-output-wk3.log /home/samnickolay/$results_directory/automating-script-output-wk3.log
# scp -r samnickolay@login.jlse.anl.gov:/home/samnickolay/automating-script-output-wk4.log /home/samnickolay/$results_directory/automating-script-output-wk4.log

##scp -r samnickolay@login.jlse.anl.gov:/home/samnickolay/cobalt-master/src/components/slowdown_results /home/samnickolay/slowdown_results-dec/
##scp -r samnickolay@login.jlse.anl.gov:/home/samnickolay/cobalt-master/src/components/slowdown_results /home/samnickolay/slowdown_results-nov/
##scp -r samnickolay@login.jlse.anl.gov:/home/samnickolay/cobalt-master/src/components/slowdown_results /home/samnickolay/slowdown_results-oct/

##scp -r /home/samnickolay/cobalt-master/ samnickolay@login.jlse.anl.gov:/home/samnickolay/cobalt-master/
##scp /home/samnickolay/cobalt-support-scripts/cobalt-automating-script-parallel.py samnickolay@login.jlse.anl.gov:/home/samnickolay/cobalt-automating-script-parallel.py


ssh samnickolay@login.jlse.anl.gov
qsub -q gomez -t 24:00:00 -n 1 -I

qsub -q gomez -t 20:00:00 -n 1 -I

qsub -q gomez -t 6:00:00 -n 1 -I


qsub -q gomez -t 6:00:00 -n 1 /home/samnickolay/cobalt-master/cobalt-support-scripts/qsub_script_v1.sh "python /home/samnickolay/cobalt-master/cobalt-support-scripts/cobalt-automating-script-parallel.py --rt_percents 5 10 15 20 --trials 5 --rt_job_categories all --checkpoint_heuristics baseline v2_sam_v1 v2p_app_sam_v1 --runtime_estimators walltime --checkp_overhead_percents 5 10 20 --log_file_name wk1.log --results_window_start 2018/01/21 --results_window_length 7"


export PYTHONPATH=/home/samnickolay/cobalt-master/src
export HOST=ubuntu
export PYTHONUNBUFFERED=1
export COBALT_CONFIG_FILES=/home/samnickolay/cobalt-master/src/components/conf/cobalt.conf
rm -r /home/samnickolay/cobalt-master/src/components/results/*


# Old Commands
rm -r /home/samnickolay/cobalt-master/src/components/slowdown_results/*
rm -r /home/samnickolay/cobalt-master/src/components/simulation_output/*

# cd /home/samnickolay/cobalt-master/src/components
# rm -r /home/samnickolay/cobalt-master/src/components/slowdown_results/*
'''


import subprocess
import sys
import time 

import argparse

parser = argparse.ArgumentParser(description='Automate running Cobalt simulations')

'''
sample script examples to test that it works correctly
python /home/samnickolay/cobalt-master/cobalt-support-scripts/cobalt-automating-script-parallel.py --rt_percents 5 10 15 20 --trials 3 --rt_job_categories all --checkpoint_heuristics baseline highpQ v1 v2 v2_sam_v1 v2p v2p_app v2p_app_sam_v1 --runtime_estimators walltime --checkp_overhead_percents 5 10 20 --log_file_name wk1.log --results_window_start 2018/01/21 --results_window_length 7 --test

python /home/samnickolay/cobalt-master/cobalt-support-scripts/cobalt-automating-script-parallel.py --rt_percents 5 10 15 20 --trials 3 --rt_job_categories all --checkpoint_heuristics baseline highpQ v1 v2 v2_sam_v1 v2p v2p_app v2p_app_sam_v1 --runtime_estimators walltime --checkp_overhead_percents 5 10 20 --log_file_name wk2.log --results_window_start 2018/03/18 --results_window_length 7 --test

python /home/samnickolay/cobalt-master/cobalt-support-scripts/cobalt-automating-script-parallel.py --rt_percents 5 10 15 20 --trials 3 --rt_job_categories all --checkpoint_heuristics baseline highpQ v1 v2 v2_sam_v1 v2p v2p_app v2p_app_sam_v1 --runtime_estimators walltime --checkp_overhead_percents 5 10 20 --log_file_name wk3.log --results_window_start 2018/06/10 --results_window_length 7 --test

python /home/samnickolay/cobalt-master/cobalt-support-scripts/cobalt-automating-script-parallel.py --rt_percents 5 10 15 20 --trials 3 --rt_job_categories all --checkpoint_heuristics baseline highpQ v1 v2 v2_sam_v1 v2p v2p_app v2p_app_sam_v1 --runtime_estimators walltime --checkp_overhead_percents 5 10 20 --log_file_name wk4.log --results_window_start 2018/03/04 --results_window_length 7 --test


just baseline - lowSD logs
python /home/samnickolay/cobalt-master/cobalt-support-scripts/cobalt-automating-script-parallel.py --rt_percents 10 --trials 1 --rt_job_categories all --checkpoint_heuristics baseline --runtime_estimators walltime --checkp_overhead_percents 5 --log_file_name wk1-lowSD.log --results_window_start 2018/01/23 --results_window_length 5 --test

python /home/samnickolay/cobalt-master/cobalt-support-scripts/cobalt-automating-script-parallel.py --rt_percents 10 --trials 1 --rt_job_categories all --checkpoint_heuristics baseline --runtime_estimators walltime --checkp_overhead_percents 5 --log_file_name wk2-lowSD.log --results_window_start 2018/03/20 --results_window_length 5 --test

python /home/samnickolay/cobalt-master/cobalt-support-scripts/cobalt-automating-script-parallel.py --rt_percents 10 --trials 1 --rt_job_categories all --checkpoint_heuristics baseline --runtime_estimators walltime --checkp_overhead_percents 5 --log_file_name wk3-lowSD.log --results_window_start 2018/06/12 --results_window_length 5 --test

'''


home_directory = '/gpfs/jlse-fs0/users/samnickolay/'

# small_simulation_test = False

small_simulation_test_log_files = ['0.log', '1.log', '2.log', '3.log', '4.log', '5.log', '6.log', '7.log', '8.log', '9.log', '10.log', 
                                   '11.log', '12.log', '13.log', '14.log', '15.log', '16.log', '17.log', '18.log', '19.log']
small_simulation_test_log_directory = home_directory + 'create-test-logs/simulation-logs/'

small_simulation_test_log_files = [small_simulation_test_log_directory + log_file for log_file in small_simulation_test_log_files]

small_partition_file_name = 'mira-8k.xml'

import datetime

start_time = datetime.datetime.now()
print('start time: ' + str(start_time))


rt_percents = ['5', '10', '15', '20']
rt_job_categories = ['all', 'short', 'narrow', 'short-and-narrow', 'short-or-narrow', 'corehours']
checkpoint_heuristics = ['baseline', 'highpQ', 'v1', 'v2', 'v2_sam_v1', 'v2p', 'v2p_sam_v1', 'v2p_app', 'v2p_app_sam_v1']
runtime_estimators = ['walltime', 'actual', 'predicted']
checkp_overhead_percents = ['5', '10', '20']

checkp_dsize = '16384'
checkp_t_internval = '3600'
application_checkpointing_percent = '50.0'


partition_file_name = 'mira.xml'

# add the arguments to the parser
parser.add_argument('--rt_percents', '-P', dest='input_rt_percents', nargs='+', choices=rt_percents,
                    help='the rt percents to simulate', required=True)

parser.add_argument('--trials', '-T', dest='input_times', type=int,
                    help='the number of trials to simulate for each configuration', required=True)

parser.add_argument('--log_file_name', '-l', dest='log_file_name', type=str,
                    help='the name of the log file to use for the simulations', required=True)

parser.add_argument('--rt_job_categories', '-C', dest='input_rt_job_categories', nargs='+', default=[], choices=rt_job_categories,
                    help="the rt job categories to simulate (default: test all categories " + str(rt_job_categories) + ")")

parser.add_argument('--checkpoint_heuristics', '-H', dest='input_checkpoint_heuristics', nargs='+', default=[], choices=checkpoint_heuristics,
                    help="the checkpoint heuristics to simulate (default: test all heuristics " + str(checkpoint_heuristics) + ")")

parser.add_argument('--runtime_estimators', '-E', dest='input_runtime_estimators', nargs='+', default=[], choices=runtime_estimators,
                    help="the checkpoint heuristics to simulate (default: test all estimators " + str(runtime_estimators) + ")")

parser.add_argument('--checkp_overhead_percents', '-O', dest='input_checkp_overhead_percents', nargs='+', default=[], choices=checkp_overhead_percents,
                    help="the checkpoint overhead % to use with application checkpointing heuristic (default: test all % " + str(checkp_overhead_percents) + ")")

parser.add_argument('--test', '-t', default=False, action='store_true',
                    help="if test flag is passed then the script will not actually run the cobalt simulations")


parser.add_argument('--small_simulation_test', '-s', default=False, action='store_true',
                    help="if small_simulation_test flag is passed then the script will run small simulation traces using small partition file")


parser.add_argument('--results_window_length', dest='results_window_length', type=int, required=True,
                    help="Specifies the length for the results window (number of days) (the time window to compute trimmed metrics)")

parser.add_argument('--results_window_start', dest='results_window_start', type=str, required=True,
                    help="Specifies the start_date for the results window (YYYY-MM-DD) (the time window to compute trimmed metrics)")

# parse the arguments
args = parser.parse_args()

input_rt_percents = args.input_rt_percents
times = str(args.input_times)
input_rt_job_categories = args.input_rt_job_categories
input_checkpoint_heuristics = args.input_checkpoint_heuristics
input_runtime_estimators = args.input_runtime_estimators
input_checkp_overhead_percents = args.input_checkp_overhead_percents
small_simulation_test = args.small_simulation_test
log_file_name = args.log_file_name

results_window_start = args.results_window_start
results_window_length = args.results_window_length

# if no values are passed then default to testing all values
if input_rt_job_categories == []:
    input_rt_job_categories = rt_job_categories

if input_checkpoint_heuristics == []:
    input_checkpoint_heuristics = checkpoint_heuristics

if input_runtime_estimators == []:
    input_runtime_estimators = runtime_estimators

if input_checkp_overhead_percents == []:
    input_checkp_overhead_percents = checkp_overhead_percents

# output the inputted
print("")
print("Simulation Configurations:")
print("trials: " + times)
print("rt_percents: " + str(input_rt_percents))
print("rt_job_categories: " + str(input_rt_job_categories))
print("checkpoint_heuristics: " + str(input_checkpoint_heuristics))
print('runtime_estimators: ' + str(input_runtime_estimators))
print('checkp_overhead_percents: ' + str(checkp_overhead_percents))
print("")
if args.test is True:
    print("!!!!!! This is a test - No cobalt simulations will be run !!!!!!")
print("")

if small_simulation_test:
        print("****** Running small simulation test with " + str(len(small_simulation_test_log_files)) + 
            " small traces using " + small_partition_file_name + " partition file ******")
print("")


def run_simulation(rt_percent, rt_job_category, checkpoint_heuristic, job_length_type, utility_function, checkp_overhead_percent='-1'):

    if small_simulation_test:
        current_log_file_names = small_simulation_test_log_files
        current_partition_file_name = small_partition_file_name

    else:
        current_log_file_names = [log_file_name]
        current_partition_file_name = partition_file_name


    for current_log_file_name in current_log_file_names:

        trimmed_current_log_file_name = current_log_file_name.split('/')[-1]
        sim_name = rt_percent + '%-' + rt_job_category + '-' + checkpoint_heuristic

        if 'sam_v1' in checkpoint_heuristic:
            sim_name += '-' + job_length_type
        if checkp_overhead_percent != '-1':
            sim_name += '-' + str(checkp_overhead_percent) + '%_overhead'

        sim_name += '-' + current_partition_file_name + '-' + trimmed_current_log_file_name

        print("--- " + sim_name + " ---")

        command_str = "/usr/bin/python2.7 qsim.py --batch --name " + sim_name + " --job " + current_log_file_name + " --backfill ff"\
        " --times " + times + " --output " + sim_name + ".log --partition " + current_partition_file_name + " -Y " + rt_percent +\
        " --checkpoint " + checkpoint_heuristic + " --checkp_dsize " + checkp_dsize + " --checkp_w_bandwidth 216"\
        " --checkp_r_bandwidth 216 --checkp_t_internval " + checkp_t_internval + " --intv_pcent " + application_checkpointing_percent + " --utility_function " + utility_function +\
        " --job_length_type " + job_length_type + " --rt_job_categories " + rt_job_category + " --overhead_checkpoint_percent " + checkp_overhead_percent

        command_str += " --results_window_length " + str(results_window_length)
        command_str += " --results_window_start " + str(results_window_start)

        print(command_str)
        print('') 

        # stdout_file = 'simulation_output/' + sim_name + '.output'

        if args.test is False:
            import os
            os.chdir(home_directory + 'cobalt-master/src/components')
            command_list = command_str.split(' ')
            # FNULL = open('/home/samnickolay/output-logs/' + sim_name + '-output-log.txt', 'w')
            # FNULL = open(os.devnull, 'w')
            # tmp_process = subprocess.Popen(command_list, stdout=FNULL, stderr=subprocess.STDOUT)

            simulation_output_folder = 'results/simulation_output/'
            if not os.path.exists(simulation_output_folder):
                os.makedirs(simulation_output_folder)

            stdout_file = open(simulation_output_folder + sim_name + '.output', 'w')

            tmp_process = subprocess.Popen(command_list, stdout=stdout_file, stderr=subprocess.STDOUT)
            # tmp_process = subprocess.Popen("sleep 180; echo 'hello'", shell=True)

            simulation_parameters = [rt_percent, rt_job_category, checkpoint_heuristic, job_length_type,
                                     utility_function, checkp_overhead_percent]

            return (tmp_process, sim_name, stdout_file, simulation_parameters)
            # global processes
            # processes.append((tmp_process, sim_name))

            # retcode = subprocess.call(command_list, stdout=FNULL, stderr=subprocess.STDOUT)
            # print(retcode)
        return None


processes = []

for rt_percent in input_rt_percents:
    print('----------------------------------------------------------------------')
    print("Testing rt percent: " + rt_percent)

    for rt_job_category in input_rt_job_categories:
        print("***********************************")
        print("Testing rt job category: " + rt_job_category  + "\n")

        for checkpoint_heuristic in input_checkpoint_heuristics:

            if checkpoint_heuristic == "baseline":
                job_length_type = "none"
                utility_function = "wfp2"

                tmp_process = run_simulation(rt_percent, rt_job_category, checkpoint_heuristic, job_length_type, utility_function)
                if tmp_process is not None:
                    processes.append(tmp_process)

            elif 'sam_v1' not in checkpoint_heuristic: # ['v2', 'v2p', 'v2p_app']
                job_length_type = "none"
                utility_function = "default"

                tmp_process = run_simulation(rt_percent, rt_job_category, checkpoint_heuristic, job_length_type, utility_function)
                if tmp_process is not None:
                    processes.append(tmp_process)

            elif 'sam_v1' in checkpoint_heuristic: # ['v2_sam_v1', 'v2p_sam_v1', 'v2p_app_sam_v1']
                for runtime_estimator in input_runtime_estimators:
                    job_length_type = runtime_estimator
                    # utility_function = "custom_v1"
                    utility_function = "wfp2"

                    if 'app' in checkpoint_heuristic:
                        for checkp_overhead_percent in input_checkp_overhead_percents:
                            tmp_process = run_simulation(rt_percent, rt_job_category, checkpoint_heuristic, job_length_type, utility_function, checkp_overhead_percent)
                            if tmp_process is not None:
                                processes.append(tmp_process)
                    else:

                        tmp_process = run_simulation(rt_percent, rt_job_category, checkpoint_heuristic, job_length_type, utility_function)
                        if tmp_process is not None:
                            processes.append(tmp_process)

            else:
                print('error on checkpoint_heuristic: ', checkpoint_heuristic)
                exit(-1)


output_tracking_file = None
if len(processes) > 0:
    output_tracking_filename = home_directory + 'automating-script-output-' + log_file_name
    output_tracking_file = open(output_tracking_filename, 'w', 1)
    output_tracking_file.write("" + '\n')
    output_tracking_file.write("Simulation Configurations:" + '\n')
    output_tracking_file.write("trials: " + times + '\n')
    output_tracking_file.write("rt_percents: " + str(input_rt_percents) + '\n')
    output_tracking_file.write("rt_job_categories: " + str(input_rt_job_categories) + '\n')
    output_tracking_file.write("checkpoint_heuristics: " + str(input_checkpoint_heuristics) + '\n')
    output_tracking_file.write('runtime_estimators: ' + str(input_runtime_estimators) + '\n')
    output_tracking_file.write('checkp_overhead_percents: ' + str(checkp_overhead_percents) + '\n')
    output_tracking_file.write('\n')

time_count = -1.0
# import datetime
while len(processes) > 0:
    time_count += 1.0
    removed_processes = []
    for process_tuple in processes:
        p, process_name, output_file, simulation_parameters = process_tuple
        poll = p.poll()
        if poll is not None:
            return_code = p.returncode
            print('')
            print(process_name)
            print(return_code)
            output_tracking_file.write(str(process_name) + '  -  ' + str(return_code) + '  -  ' + str(datetime.datetime.now()) + '\n')
            if return_code != 0:
                error_string = ('**************************************************\n'
                                '********** Error in simulation! Non 0 exit code! **********\n'
                                'Check file for error in "simulation_output" folder \n'
                                '**************************************************\n')
                print(error_string)
                output_tracking_file.write(error_string)

            removed_processes.append(process_tuple)
    for removed_process in removed_processes:
        removed_process[2].close()
        processes.remove(removed_process)
    print(str(len(processes)) + ' - ' + str(time_count) + ' - ' + str(datetime.datetime.now()))
    output_tracking_file.write(str(len(processes)) +' - '+ str(time_count) +' - '+ str(datetime.datetime.now()) + '\n')

    if len(processes) > 0:
        time.sleep(60)


if output_tracking_file is not None:
    output_tracking_file.write('\ndone!')
    output_tracking_file.close()


print('done!')


end_time = datetime.datetime.now()
print('end time: ' + str(end_time))
print('run time: ' + str(end_time-start_time))