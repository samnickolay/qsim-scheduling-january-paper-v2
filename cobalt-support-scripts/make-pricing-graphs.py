
import argparse
import datetime
import os
import pickle
import csv

import matplotlib.pyplot as plt
from matplotlib.pyplot import setp, plot
import numpy as np

# python pickle-parsing-script.py
# python cobalt-master/cobalt-support-scripts/pickle-parsing-script.py -O job_metric_spreadsheets -P test.pickle

total_nodes = 49152


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

current_pickle_folder = 'results_2-11-19_v1'

input_pickle_folder = '/home/samnickolay/' + current_pickle_folder +'/simulation_pickled_data/'
output_folder = '/home/samnickolay/pricing_output/' + current_pickle_folder + '/'
number_trials = 5
current_log = 'wk1.log'

rt_percents = ['5%', '10%', '15%', '20%']
checkpoint_heuristics = ['baseline', 'v2_sam_v1', 'v2p_app_sam_v1']

runtime_estimators = ['walltime'] #['walltime', 'actual', 'predicted']
checkp_overhead_percents = ['5', '10', '20']

extra_metrics = ['makespan', 'system_utilization', 'productive_utilization', 'overhead_utilization']

job_sizes = ['all', 'narrow_short', 'narrow_long', 'wide_short', 'wide_long']

job_values_needed = ('trimmed','turnaround_time','log_run_time', 'nodes', 'job_type', 'job_category', 'queued_time',
                     'end_time', 'slowdown_walltime', 'slowdown_runtime')

reimbursement_thresholds = [0.0, 0.1, 0.2, 0.3]

online_pricing_window = datetime.timedelta(days=1)

checkpoint_heuristics_to_compare = [{'checkpoint_heuristic': 'v2p_app_sam_v1', 'checkpoint_overhead': '5%_overhead',
                                     'runtime_estimator': 'walltime'},
                                    {'checkpoint_heuristic': 'v2p_app_sam_v1', 'checkpoint_overhead': '10%_overhead',
                                     'runtime_estimator': 'walltime'},
                                    {'checkpoint_heuristic': 'v2_sam_v1', 'runtime_estimator': 'walltime'}]
checkpoint_heuristics_to_graph_names = ['SYS-APP-5%', 'SYS-APP-10%', 'JIT-CKPT']

checkpoint_heuristic_baseline = {'checkpoint_heuristic': 'baseline'}


if not os.path.exists(output_folder):
    print('Output folder provided does not exist - creating output folder')
    print(output_folder)
    os.makedirs(output_folder)


def main():
    print('Parsing pickled files - looking for ' + str(number_trials) + ' trials')
    pickle_dict = {}

    for sub_filename in os.listdir(input_pickle_folder):
        if sub_filename.split('.')[-1] == 'pickle':
            if ('v2p_app_sam_v1' not in sub_filename and 'v2_sam_v1' not in sub_filename
                and 'baseline' not in sub_filename) or current_log not in sub_filename:
                continue

            pickle_data = load_pickle_data(os.path.join(input_pickle_folder, sub_filename))
            pickle_data = remove_unnecessary_data(pickle_data, job_values_needed)
            pickle_name = make_pickle_name(sub_filename)
            # pickle_name = sub_filename.split('.')[0]
            dict_key = tuple(sorted(pickle_name.items()))
            pickle_dict[dict_key] = pickle_data
            print('loaded - ' + sub_filename)


    for rt_percent in rt_percents:
        for trial_number in range(number_trials):
            extra_filters = {'rt_percent': rt_percent, 'trial_number': 'trial' + str(trial_number), 'log_file': current_log}
            baseline_data_dict = get_data_dict_verified(pickle_dict, checkpoint_heuristic_baseline, extra_filters)

            for checkpoint_heuristic in checkpoint_heuristics_to_compare:
                current_data_dict = get_data_dict_verified(pickle_dict, checkpoint_heuristic, extra_filters)

                for reimbursement_threshold in reimbursement_thresholds:
                    print('computing pricing for - ' + str(rt_percent) + ' - ' + str(trial_number) + ' - ' + 
                          str(checkpoint_heuristic) + ' - ' + str(reimbursement_threshold))
                    compute_offline_pricing_values(current_data_dict, baseline_data_dict, reimbursement_threshold)
                    
    from 'online-pricing-metrics.py' import compute_online_pricing_metrics
    compute_online_pricing_metrics(pickle_dict)

    print_performance_cost_difference_deficit(checkpoint_heuristics_to_compare, pickle_dict)

    for job_size in job_sizes:
        output_folder = '/home/samnickolay/pricing-plots/'
        current_data = current_log.split('.')[0]
        plot_type = 'box'
        output_file_name = output_folder + plot_type + '/' + current_data + '-' + job_size.strip('_') + '.png'
        make_pricing_graph(checkpoint_heuristics_to_compare, checkpoint_heuristics_to_graph_names, output_file_name, job_size,
                   plot_type, current_log, number_trials, pickle_dict, log_y=False, max_min_dots=False,)

        plot_type = 'box'
        output_file_name = output_folder + plot_type + '-log_y/' + current_data + '-' + job_size.strip('_') + '.png'
        make_pricing_graph(checkpoint_heuristics_to_compare, checkpoint_heuristics_to_graph_names, output_file_name, job_size,
                   plot_type, current_log, number_trials, pickle_dict, log_y=True, max_min_dots=False)

        plot_type = 'box'
        output_file_name = output_folder + plot_type + '-max_min_dots/' + current_data + '-' + job_size.strip('_') + '.png'
        make_pricing_graph(checkpoint_heuristics_to_compare, checkpoint_heuristics_to_graph_names, output_file_name, job_size,
                   plot_type, current_log, number_trials, pickle_dict, log_y=False, max_min_dots=True)

        plot_type = 'box'
        output_file_name = output_folder + plot_type + '-log_y-max_min_dots/' + current_data + '-' + job_size.strip('_') + '.png'
        make_pricing_graph(checkpoint_heuristics_to_compare, checkpoint_heuristics_to_graph_names, output_file_name, job_size,
                   plot_type, current_log, number_trials, pickle_dict, log_y=True, max_min_dots=True)

        #######################
        # output_folder = '/home/samnickolay/plots/'
        # plot_type = 'average'
        # output_file_name = output_folder + plot_type + '/' + current_data + '-' + job_size.strip('_') + '.png'
        # make_graph(checkpoint_heuristics_to_graph, checkpoint_heuristics_to_graph_names, output_file_name, job_size,
        #            plot_type, log_y=False, max_min_dots=False)


    #
    # pickle_filename_base = os.path.basename(pickle_filename)
    # spreadsheet_filename = os.path.join(output_folder, os.path.splitext(pickle_filename_base)[0]) + '.csv'
    # make_job_metrics_spreadsheet(jobs_metrics, spreadsheet_filename)
    #
    # compute_pricing_values =

def remove_unnecessary_data(pickle_dict, job_values_needed):
    jobs_metrics_current = pickle_dict['jobs_metrics']
    new_jobs_metrics_current = {}
    for jobid, job in jobs_metrics_current.iteritems():
        if job['trimmed'] == False:
            continue
        new_jobs_metrics_current[jobid] = job

        # temp_job_dict = {}
        # for value in job_values_needed:
        #     if value == 'end_time':
        #         temp_job_dict[value] = job['end_times'][-1]
        #     else:
        #         temp_job_dict[value] = job[value]
        # new_jobs_metrics_current[jobid] = temp_job_dict

    new_pickle_dict = {'jobs_metrics': new_jobs_metrics_current}
    return new_pickle_dict


def get_matching_jobs_values(current_data_dict, job_size, current_metric, job_type=None):
    jobs_metrics_current = current_data_dict['jobs_metrics']

    job_values = []
    for jobid, job in jobs_metrics_current.iteritems():
        if job['trimmed'] == False:
            continue

        if job_size != 'all' and job_size != job['job_category']:
            continue

        if job_type is not None and job['job_type'].lower() != job_type.lower():
            continue

        if current_metric not in job:
            print('invalid current metric - ' + current_metric)
            exit(-1)

        current_job_value = job[current_metric]

        job_values.append(current_job_value)

    return job_values


def print_performance_cost_difference_deficit(checkpoint_heuristics_to_graph, pickle_dict):
    job_size = 'all'
    for reimbursement_threshold in reimbursement_thresholds:
        print('reimbursement_threshold - ' + str(reimbursement_threshold))
        for rt_percent in rt_percents:
            for checkpoint_heuristic in checkpoint_heuristics_to_graph:
                # for current_metric in plotting_metrics_even:
                # current_values_temp = []
                for trial_number in range(number_trials):
                    extra_filters = {'rt_percent': rt_percent, 'trial_number': 'trial' + str(trial_number)}
                    current_data_dict = get_data_dict_verified(pickle_dict, checkpoint_heuristic, extra_filters)

                    cost_performance_name_online = 'cost-performance-' + str(
                        reimbursement_threshold) + 'slack' + '-online'

                    current_values_temp = get_matching_jobs_values(current_data_dict, job_size,
                                                                    cost_performance_name_online, job_type='rt')
                    total_online_value_rt = float(sum(current_values_temp))

                    current_values_temp = get_matching_jobs_values(current_data_dict, job_size,
                                                                  cost_performance_name_online, job_type='batch')
                    total_online_value_batch = float(sum(current_values_temp))

                    cost_performance_name_offline = 'cost-performance-' + str(
                        reimbursement_threshold) + 'slack' + '-offline'

                    current_values_temp = get_matching_jobs_values(current_data_dict, job_size,
                                                                    cost_performance_name_offline, job_type='rt')
                    total_offline_value_rt = float(sum(current_values_temp))

                    current_values_temp = get_matching_jobs_values(current_data_dict, job_size,
                                                                   cost_performance_name_offline, job_type='batch')
                    total_offline_value_batch = float(sum(current_values_temp))
                    
                    
                    # # print(str(rt_percent) + ' - ' + str(checkpoint_heuristic) + ' - ' + job_size + ' - trial'
                    # #       + str(trial_number) + ' - ' + str(total_online_value) + ' - ' +
                    # #       str(total_offline_value) + ' - ' + str(total_offline_value-total_online_value))
                    # print(str(rt_percent) + ', ' + str(checkpoint_heuristic).replace(', ', ' - ') + ', ' + job_size + ', trial '
                    #       + str(trial_number) + ', ' + str(total_online_value) + ', ' +
                    #       str(total_offline_value) + ', ' + str(total_offline_value-total_online_value))
                    # 
                    # print("-- Online Cost - RT = " + str(total_online_value_rt))
                    # print("-- Online Cost - Batch = " + str(total_online_value_batch))
                    # print("-- Offline Cost - RT = " + str(total_offline_value_rt))
                    # print("-- Offline Cost - Batch = " + str(total_offline_value_batch))
                    # print("-- Online Cost  = " + str(total_online_value_rt))


# make graph
def make_pricing_graph(checkpoint_heuristics_to_graph, checkpoint_heuristics_to_graph_names, output_file_name, job_size,
               plot_type, current_log, number_trials, pickle_dict, log_y, max_min_dots):

    # plotting_metrics = ['RT Cost Original','Batch Cost Original', 'RT Cost New - Even','Batch Cost New' ]
    # plotting_metrics = ['RT Cost Original','Batch Cost Original', 'RT Cost New - Performance','Batch Cost New' ]
    # plotting_metrics = ['RT Cost New - Even', 'Batch Cost Original', 'RT Cost New - Performance','Batch Cost New' ]

    plotting_metric_names_performance_offline = ['RT - Performance - Offline - 0% Slack', 'RT - Performance - Offline - 10% Slack',
                        'RT - Performance - Offline - 20% Slack', 'RT - Performance - Offline - 30% Slack']
    plotting_metric_names_even_offline = ['RT - Even - Offline - 0% Slack', 'RT - Even - Offline - 10% Slack',
                        'RT - Even - Offline - 20% Slack', 'RT - Even - Offline - 30% Slack']
    # plotting_metrics = ['Performance - 0% Slack', 'Performance - 10% Slack',
    #                     'Performance - 20% Slack', 'Performance - 30% Slack']

    plotting_metric_names_performance_online = ['RT - Performance - Online - 0% Slack', 'RT - Performance - Online - 10% Slack',
                        'RT - Performance - Online - 20% Slack', 'RT - Performance - Online - 30% Slack']
    plotting_metric_names_performance_online_offline = ['RT - Performance - Online - 0% Slack', 'RT - Performance - Online - 10% Slack',
                        'RT - Performance - Online - 20% Slack', 'RT - Performance - Online - 30% Slack']

    plotting_metric_names_performance_online_estimate = ['RT - Online - 0% Slack',
                                                        'RT - Online - 10% Slack',
                                                        'RT - Online - 20% Slack',
                                                        'RT - Online - 30% Slack']

    plotting_metrics_performance_offline = []
    plotting_metrics_even_offline = []
    plotting_metrics_performance_online = []
    # plotting_metrics_performance_online_offline = []
    plotting_metrics_performance_online_estimate = []


    for reimbursement_threshold in reimbursement_thresholds:
        plotting_metrics_performance_offline.append('cost-ratio-performance-' + str(reimbursement_threshold) + 'slack' + '-offline')
        plotting_metrics_even_offline.append('cost-ratio-even-' + str(reimbursement_threshold) + 'slack' + '-offline')

        plotting_metrics_performance_online.append('cost-ratio-performance-' + str(reimbursement_threshold) + 'slack' + '-online')
        # plotting_metrics_performance_online_offline.append('cost-ratio-performance-' + str(reimbursement_threshold) + 'slack' + '-online-offline')

        plotting_metrics_performance_online_estimate.append('cost-estimate-ratio-performance-' + str(reimbursement_threshold) + 'slack' + '-online')

    # cost_estimate_ratio_performance_name = 'cost-estimate-ratio-performance-' + specifier_str


    # pricing_method = 'even-offline'
    # pricing_method = 'performance-offline'
    # pricing_method = 'performance-online'
    pricing_method = 'performance-online-estimate'



    # pricing_method = 'performance-online-offline'

    y_labels = ['Cost Ratio (New/Old)','Cost Ratio (New/Old)',
                'Cost Ratio (New/Old)','Cost Ratio (New/Old)']

    if pricing_method == 'even-offline':
        plotting_metric_names = plotting_metric_names_even_offline
        plotting_metrics = plotting_metrics_even_offline

    elif pricing_method == 'performance-offline':
        plotting_metric_names = plotting_metric_names_performance_offline
        plotting_metrics = plotting_metrics_performance_offline

    elif pricing_method == 'performance-online':
        plotting_metric_names = plotting_metric_names_performance_online
        plotting_metrics = plotting_metrics_performance_online

    # elif pricing_method == 'performance-online-offline':
    elif pricing_method == 'performance-online-estimate':

        plotting_metric_names = plotting_metric_names_performance_online_estimate
        plotting_metrics = plotting_metrics_performance_online_estimate

        y_labels = ['Cost Ratio (Actual/Estimated)', 'Cost Ratio (Actual/Estimated)',
                'Cost Ratio (Actual/Estimated)', 'Cost Ratio (Actual/Estimated)']

    # for rt_percent in rt_percents:
    #     for checkpoint_heuristic in checkpoint_heuristics_to_graph:
    #         # for current_metric in plotting_metrics_even:
    #         # current_values_temp = []
    #         for trial_number in range(number_trials):
    #             extra_filters = {'rt_percent': rt_percent, 'trial_number': 'trial' + str(trial_number)}
    #             current_data_dict = get_data_dict_verified(pickle_dict, checkpoint_heuristic, extra_filters)
    #
    #             cost_performance_name_online = 'cost-performance-' + str(
    #                 reimbursement_threshold) + 'slack' + '-online'
    #
    #             current_values_temp = get_matching_jobs_values(current_data_dict, job_size,
    #                                                             cost_performance_name_online, job_type='rt')
    #             total_online_value = float(sum(current_values_temp))
    #
    #             cost_performance_name_offline = 'cost-performance-' + str(
    #                 reimbursement_threshold) + 'slack' + '-offline'
    #
    #             current_values_temp = get_matching_jobs_values(current_data_dict, job_size,
    #                                                             cost_performance_name_offline, job_type='rt')
    #             total_offline_value = float(sum(current_values_temp))
    #             # print(str(rt_percent) + ' - ' + str(checkpoint_heuristic) + ' - ' + job_size + ' - trial'
    #             #       + str(trial_number) + ' - ' + str(total_online_value) + ' - ' +
    #             #       str(total_offline_value) + ' - ' + str(total_offline_value-total_online_value))
    #             print(str(rt_percent) + ', ' + str(checkpoint_heuristic).replace(', ', ' - ') + ', ' + job_size + ', trial '
    #                   + str(trial_number) + ', ' + str(total_online_value) + ', ' +
    #                   str(total_offline_value) + ', ' + str(total_offline_value-total_online_value))
    #
    #             # average_value = float(sum(current_values_temp)) / len(current_values_temp)
    #             #
    #             # specifier_str = str(reimbursement_threshold) + 'slack' + '-online'
    #             # cost_performance_name_online = 'cost-performance-' + str(reimbursement_threshold) + 'slack' + '-online'
    #             # cost_performance_name_offline = 'cost-performance-' + str(reimbursement_threshold) + 'slack' + '-offline'
    #             #
    #             # cost_ratio_performance_online_offline_name = 'cost-ratio-performance-' + specifier_str + '-offline'
    #             #
    #             # print(str(rt_percent) + ' - ' + str(checkpoint_heuristic) + ' - ' + job_size + ' - ' + current_metric +
    #             #       ' - - ' + str(average_value))

    # y_labels = ['Cost (Node-Hours)', 'Cost (Node-Hours)', 'Cost (Node-Hours)', 'Cost (Node-Hours)']


    colors = ['b', 'r', 'g', 'm', 'y', 'k']

    # plot_type = 'average'
    # plot_type = 'box'
    if plot_type not in ['average', 'box']:
        print('invalid plot type')
        print(plot_type)
        exit(-1)

    print('making figure: ' + output_file_name)

    # for job_size in job_sizes:
    # make a figure

    f, axarr = plt.subplots(2, 2, figsize=(9, 6))

    for idx, current_metric in enumerate(plotting_metrics):

        # current_metric = job_size + plotting_metric
        # print('current metric: ' + current_metric)

        axis0 = int(idx / 2)
        axis1 = idx % 2

        current_ax = axarr[axis0, axis1]

        rects_list = []

        offset = 0.0
        color_idx = 0

        x_axis_numbers = np.arange(len(rt_percents))

        #####################################################33

        # if plot_type == 'average':
        #     width = 0.18
        #     for checkpoint_heuristic_to_graph in checkpoint_heuristics_to_graph:
        #         current_values = []
        #         for rt_percent in rt_percents:
        #             tmp_data_dict = get_data_dict_verified(checkpoint_heuristic_to_graph, rt_percent)
        #             values_list = tmp_data_dict['metrics'][current_metric]
        #             current_value = float(sum(values_list)) / len(values_list)
        #             current_values.append(current_value)
        #
        #         current_rects = current_ax.bar(x_axis_numbers + offset, current_values, width=width,
        #                                        color=colors[color_idx])  # , width, color='r', yerr=men_std)
        #         rects_list.append(current_rects)
        #
        #         offset += width
        #         color_idx += 1
        #
        #     # add some text for labels, title and axes ticks
        #     if 'slowdown' in current_metric:
        #         current_ax.set_ylabel('Avg Slow Down (x times)', fontweight='bold')
        #     else:
        #         current_ax.set_ylabel('Avg Turnaround Time (min)', fontweight='bold')
        #     current_ax.set_xlabel('Realtime Job Percentage (%)', fontweight='bold')
        #     current_ax.set_xticklabels(rt_percents)
        #     current_ax.set_xticks(x_axis_numbers + width * 2)
        #
        #     if log_y is True:
        #         current_ax.set_yscale('log')
        #
        #         from matplotlib.ticker import ScalarFormatter
        #         current_ax.yaxis.set_major_formatter(ScalarFormatter())
        #
        #     f.legend(rects_list, checkpoint_heuristics_to_graph_names, loc=(0.12, 0.94), ncol=5)

        ############################
        if plot_type == 'box':
            count = -1
            width = 0.6
            for rt_percent in rt_percents:
                count += 1
                # current_values = []
                # for checkpoint_heuristic_to_graph in checkpoint_heuristics_to_graph:
                #     tmp_data_dict = get_data_dict_verified(checkpoint_heuristic_to_graph, rt_percent)
                #     values_list = tmp_data_dict['metrics'][current_metric]
                #     current_values.append(values_list)

                current_values = []
                for checkpoint_heuristic in checkpoint_heuristics_to_graph:
                    current_values_temp = []
                    for trial_number in range(number_trials):
                        extra_filters = {'rt_percent': rt_percent, 'trial_number': 'trial' + str(trial_number)}
                        current_data_dict = get_data_dict_verified(pickle_dict, checkpoint_heuristic, extra_filters)

                        current_values_temp += get_matching_jobs_values(current_data_dict, job_size, current_metric,
                                                                        job_type='rt')
                    current_values.append(current_values_temp)

                heuristic_numbers = np.arange(len(checkpoint_heuristics_to_graph))
                current_positions = heuristic_numbers + (count * (len(heuristic_numbers) + 1)) + 1
                bp = current_ax.boxplot(current_values, positions=current_positions, widths=width,
                                        whis=[5, 95], showfliers=False)

                if max_min_dots is True:
                    for i in heuristic_numbers:
                        min_value = min(current_values[i])
                        max_value = max(current_values[i])
                        size = 1.5
                        current_ax.plot(current_positions[i], min_value, colors[i] + 'o', markersize=size)
                        current_ax.plot(current_positions[i], max_value, colors[i] + 'o', markersize=size)

                for i in heuristic_numbers:
                    idx1 = i
                    idx2 = 2 * i
                    idx3 = 2 * i + 1
                    setp(bp['boxes'][idx1], color=colors[i])
                    setp(bp['medians'][idx1], color=colors[i])
                    setp(bp['caps'][idx2], color=colors[i])
                    setp(bp['caps'][idx3], color=colors[i])
                    setp(bp['whiskers'][idx2], color=colors[i])
                    setp(bp['whiskers'][idx3], color=colors[i])
                    # setp(bp['fliers'][idx2], color=colors[i])
                    # setp(bp['fliers'][idx3], color=colors[i])

                color_idx += 1

            # add some text for labels, title and axes ticks
            # if 'New' in current_metric:
            #     # current_ax.set_ylabel('Cost (Node-Hours)', fontweight='bold')
            #     current_ax.set_ylabel('Cost Change Ratio (New/Old)', fontweight='bold')
            #
            # else:
            #     current_ax.set_ylabel('Cost (Node-Hours)', fontweight='bold')
            current_ax.set_ylabel(y_labels[idx], fontweight='bold')

            current_ax.set_xlabel('Realtime Job Percentage (%)', fontweight='bold')
            current_ax.set_xticklabels(rt_percents)
            # current_ax.set_xticks(x_axis_numbers*width*(len(checkpoint_heuristics_to_graph)+1) + 1)
            # current_ax.set_xticks([0.0, 3.0, 8.0, 13.0, 18.0])
            current_ax.set_xticks([0.0, 2.0, 7.0, 12.0, 17.0])
            current_ax.set_xticks(x_axis_numbers * (len(checkpoint_heuristics_to_graph) + 1) + 3.0)

            if log_y is True:
                current_ax.set_yscale('log')

                from matplotlib.ticker import ScalarFormatter
                current_ax.yaxis.set_major_formatter(ScalarFormatter())

            lines = []
            for color in colors:
                l, = plot([0.5, 0.5], color + '-')
                lines.append(l)
            f.legend(lines, checkpoint_heuristics_to_graph_names, loc=(0.3, 0.94), ncol=5)

            for line in lines:
                line.set_visible(False)
        ########################################################3

        current_title = current_ax.set_title(plotting_metric_names[idx], fontsize=15)
        # current_title = current_ax.set_title(job_size + ' - ' + plotting_metric_names[idx], fontsize=15)

        current_title.set_position([.48, -0.45])

    # f.legend(rects_list, checkpoint_heuristics_to_graph_names, loc=(0.12, 0.94), ncol=5)

    # f.subplots_adjust(top=0.92, bottom=0.5, left=0.5, right=0.95, hspace=0.75, wspace=0.75)
    # plt.subplots_adjust(top=0.92, bottom=0.15, left=0.08, right=0.98, hspace=0.55, wspace=0.25)
    plt.subplots_adjust(top=0.92, bottom=0.15, left=0.11, right=0.96, hspace=0.55, wspace=0.3)


    # f.savefig('foo.png')

    # try:
    #     os.makedirs(output_file_name)
    # except OSError:
    #     if not os.path.isdir(output_file_name):
    #         raise
    import errno

    if not os.path.exists(os.path.dirname(output_file_name)):
        try:
            os.makedirs(os.path.dirname(output_file_name))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    f.savefig(output_file_name)
    plt.close(f)
    # exit()


def compute_offline_pricing_values(current_data_dict, baseline_data_dict, reimbursement_threshold):
    jobs_metrics_current = current_data_dict['jobs_metrics']
    jobs_metrics_baseline = baseline_data_dict['jobs_metrics']

    total_reimbursed_core_hours = 0.0
    total_extra_cost_core_hours_even = 0.0
    total_extra_cost_core_hours_performance = 0.0

    specifier_str = str(reimbursement_threshold) + 'slack' + '-offline'
    extra_cost_core_hours_performance_name = 'extra_cost_core_hours-performance-' + specifier_str
    extra_cost_core_hours_even_name = 'extra_cost_core_hours-even-' + specifier_str
    cost_performance_name = 'cost-performance-' + specifier_str
    cost_even_name = 'cost-even-' + specifier_str
    cost_name = 'cost-' + specifier_str

    cost_ratio_performance_name = 'cost-ratio-performance-' + specifier_str
    cost_ratio_even_name = 'cost-ratio-even-' + specifier_str


    for jobid, job in jobs_metrics_current.iteritems():

        job_baseline = jobs_metrics_baseline[jobid]

        core_hour_difference = (job['turnaround_time'] - job_baseline['turnaround_time']) * job['nodes'] / 60.0

        job_core_hours = job_baseline['log_run_time'] * job['nodes'] / 60.0
        performance_core_hours = job_baseline['turnaround_time'] / job['turnaround_time'] * job_core_hours / 60.0

        job['cost_type'] = None

        job['cost_original'] = job_core_hours

        core_hour_difference_threshold = job_baseline['turnaround_time'] * job['nodes'] / 60.0 * reimbursement_threshold

        # if core_hour_difference > 0.0:
        if core_hour_difference > core_hour_difference_threshold:
            core_hour_difference -= core_hour_difference_threshold
            total_reimbursed_core_hours += core_hour_difference
            job['cost_type'] = 'reimbursed'
            job['reimbursed_core_hours'] = core_hour_difference
            job[cost_name] = job['cost_original'] - core_hour_difference

        elif job['job_type'] == 'rt' and core_hour_difference < 0.0:
            job['cost_type'] = 'charged_extra'

            job[extra_cost_core_hours_performance_name] = abs(performance_core_hours)
            job[extra_cost_core_hours_even_name] = abs(core_hour_difference)

            total_extra_cost_core_hours_even += job[extra_cost_core_hours_even_name]
            total_extra_cost_core_hours_performance += job[extra_cost_core_hours_performance_name]

        else:
            job['cost_type'] = 'normal'
            job[cost_name] = job['cost_original']

    for jobid, job in jobs_metrics_current.iteritems():
        if job['cost_type'] != 'charged_extra':
            job[cost_even_name] = job[cost_name]
            job[cost_performance_name] = job[cost_name]
            continue

        extra_cost_even = job[extra_cost_core_hours_even_name] / total_extra_cost_core_hours_even * total_reimbursed_core_hours
        extra_cost_performance = job[extra_cost_core_hours_performance_name] / total_extra_cost_core_hours_performance * total_reimbursed_core_hours

        job[cost_even_name] = job['cost_original'] + extra_cost_even
        job[cost_performance_name] = job['cost_original'] + extra_cost_performance

    # even_ratios = []
    for jobid, job in jobs_metrics_current.iteritems():
        if cost_performance_name in job:
            job[cost_ratio_performance_name] = job[cost_performance_name] / job['cost_original']
        else:
            job[cost_ratio_performance_name] = job[cost_name] / job['cost_original']

        if cost_even_name in job:
            job[cost_ratio_even_name] = job[cost_even_name] / job['cost_original']
            # even_ratios.append(job[cost_ratio_even_name])
            # if job[cost_ratio_even_name] < 0:
            #     pass
        else:
            job[cost_ratio_even_name] = job[cost_name] / job['cost_original']
            # if job[cost_ratio_even_name] < 0:
            #     pass

    # print(even_ratios)
    # even_ratios = []
    # for jobid, job in jobs_metrics_current.iteritems():
    #     if job['job_type'] == 'rt':
    #         even_ratios.append(job[cost_ratio_even_name])
    #
    # print(even_ratios)


def get_recent_jobs(jobs_metrics, metric, window_start, window_end):
    recent_jobs = {}
    for jobid, job in jobs_metrics.iteritems():
        if window_start <= job[metric]and job[metric] <= window_end:
            recent_jobs[jobid] = job

    return recent_jobs



# uses last 24 hours of jobs to compute estimated slowdown
def compute_online_pricing_values_v1(current_data_dict, baseline_data_dict, reimbursement_threshold, time_window):
    jobs_metrics_current = current_data_dict['jobs_metrics']
    jobs_metrics_baseline = baseline_data_dict['jobs_metrics']

    specifier_str = str(reimbursement_threshold) + 'slack' + '-online'
    extra_cost_core_hours_performance_name = 'extra_cost_core_hours-performance-' + specifier_str
    extra_cost_core_hours_even_name = 'extra_cost_core_hours-even-' + specifier_str
    cost_performance_name = 'cost-performance-' + specifier_str
    cost_even_name = 'cost-even-' + specifier_str
    cost_name = 'cost-' + specifier_str

    slowdown_estimate_name = 'slowdown-estimate-' + specifier_str
    cost_estimate_performance_name = 'cost-estimate-performance-' + specifier_str
    cost_estimate_even_name = 'cost-estimate-even-' + specifier_str

    cost_ratio_performance_name = 'cost-ratio-performance-' + specifier_str
    cost_ratio_performance_online_offline_name = 'cost-ratio-performance-' + specifier_str + '-offline'

    cost_estimate_ratio_performance_name = 'cost-estimate-ratio-performance-' + specifier_str


    # cost_ratio_even_name = 'cost-ratio-even-' + specifier_str

    # get all of the job data
    job_tuples = []
    for jobid, job in jobs_metrics_current.iteritems():
        temp_tuple = (job['queued_time'], jobid)
        job_tuples.append(temp_tuple)

    job_tuples.sort()

    for job_queued_time, jobid in job_tuples:
        current_job = jobs_metrics_current[jobid]
        current_job_baseline = jobs_metrics_baseline[jobid]

        if current_job['job_type'] != 'rt':
            continue

        window_end = current_job['queued_time']
        window_start = window_end - time_window.total_seconds()

        # get all slowdowns for rt jobs that finished in the last 24 hours
        recent_finished_jobs = get_recent_jobs(jobs_metrics_current, 'end_time', window_start, window_end)
        recent_rt_slowdowns =  [job['slowdown'] for jobid, job in recent_finished_jobs.iteritems() if job['job_type'] == 'rt']

        # get all slowdowns for jobs from the baseline that finished in the last 24 hours
        # recent_finished_jobs_baseline = get_recent_jobs(jobs_metrics_baseline, 'end_time', window_start, window_end)
        # recent_baseline_slowdowns =  [job['slowdown'] for jobid, job in recent_finished_jobs_baseline.iteritems()]

        # get baseline slowdown values for rt jobs that finished in the last 24 hours
        recent_finished_jobs_ids = [jobid for jobid, job in recent_finished_jobs.iteritems()]
        recent_baseline_slowdowns =  [jobs_metrics_baseline[tmp_id]['slowdown'] for tmp_id in recent_finished_jobs_ids]

        if len(recent_rt_slowdowns) > 0:
            average_recent_rt_slowdown = float(sum(recent_rt_slowdowns)) / len(recent_rt_slowdowns)
        else:
            average_recent_rt_slowdown = 1.0

        if len(recent_baseline_slowdowns) > 0:
            average_recent_baseline_slowdown = float(sum(recent_baseline_slowdowns)) / len(recent_baseline_slowdowns)
        else:
            average_recent_baseline_slowdown = 1.0

        current_job[slowdown_estimate_name] = average_recent_rt_slowdown

        job_core_hours = current_job_baseline['log_run_time'] * current_job['nodes'] / 60.0
        performance_job_core_hours = average_recent_baseline_slowdown/average_recent_rt_slowdown * job_core_hours / 60.0

        if performance_job_core_hours < 0:
            current_job[extra_cost_core_hours_performance_name] = 0
        else:
            current_job[extra_cost_core_hours_performance_name] = performance_job_core_hours

        # get performance_job_core_hours for all rt jobs that queued in the last 24 hours
        recent_queue_jobs = get_recent_jobs(jobs_metrics_current, 'queued_time', window_start, window_end)

        recent_performance_core_hours = sum([job[extra_cost_core_hours_performance_name] for jobid, job in
                                                 recent_queue_jobs.iteritems() if job['job_type'] == 'rt'])

        # get extra reimbursement hours for all batch jobs that finished in the last 24 hours
        recent_reimbursement_core_hours = 0
        for jobid, job in recent_finished_jobs.iteritems():
            if job['job_type'] == 'batch':
                job_baseline = jobs_metrics_baseline[jobid]

                core_hour_difference = (job['turnaround_time'] - job_baseline['turnaround_time']) * job['nodes'] / 60.0
                core_hour_difference_threshold = job_baseline['turnaround_time'] * job[
                    'nodes'] / 60.0 * reimbursement_threshold
                if core_hour_difference > core_hour_difference_threshold:
                    core_hour_difference -= core_hour_difference_threshold
                    recent_reimbursement_core_hours += core_hour_difference

        extra_core_hours = performance_job_core_hours / recent_performance_core_hours * recent_reimbursement_core_hours

        current_job[cost_estimate_performance_name] = current_job['cost_original'] + extra_core_hours

        # if the job had worse performance than we expected - reimburse it based on its quote price
        if current_job['slowdown'] > current_job[slowdown_estimate_name] * (1.0 + reimbursement_threshold):
            # current_job[cost_estimate_performance_name]
            #
            # job_core_hours = job_baseline['log_run_time'] * job['nodes'] / 60.0
            #
            # core_hour_original = job_baseline['log_run_time'] * job_baseline['slowdown'] * job['nodes'] / 60.0
            # core_hour_sim = job_baseline['log_run_time'] * current_job['slowdown'] * job['nodes'] / 60.0
            # core_hour_sim - core_hour_original
            #
            # slope = (current_job[cost_estimate_performance_name] - current_job['cost_original']) / \
            #         (current_job['slowdown'] - current_job[slowdown_estimate_name])

            core_hour_difference = current_job_baseline['log_run_time'] * \
                                   (current_job['slowdown'] - current_job[slowdown_estimate_name] * (1.0 + reimbursement_threshold)) * job['nodes'] / 60.0

            current_job[cost_performance_name] = current_job[cost_estimate_performance_name] - core_hour_difference

        else:
            current_job[cost_performance_name] = current_job[cost_estimate_performance_name]

        # if current_log <
        # current_job[cost_performance_name] = current_job['cost_original'] + extra_core_hours


        cost_performance_name_offline = 'cost-performance-' + str(reimbursement_threshold) + 'slack' + '-offline'

        current_job[cost_ratio_performance_name] = current_job[cost_performance_name] / current_job['cost_original']

        if cost_performance_name_offline in current_job:
            offline_cost = current_job[cost_performance_name_offline]
        else:
            offline_cost = current_job['cost-' + str(reimbursement_threshold) + 'slack' + '-offline']

        current_job[cost_ratio_performance_online_offline_name] = current_job[cost_performance_name] / offline_cost
        current_job[cost_estimate_ratio_performance_name] = current_job[cost_performance_name] / current_job[cost_estimate_performance_name]
        # if current_job[cost_ratio_performance_online_offline_name] < 0:
        #     pass


# def compute_online_pricing_values(current_data_dict, baseline_data_dict, reimbursement_threshold, time_window):
#     jobs_metrics_current = current_data_dict['jobs_metrics']
#     jobs_metrics_baseline = baseline_data_dict['jobs_metrics']
#
#     specifier_str = str(reimbursement_threshold) + 'slack' + '-online'
#     extra_cost_core_hours_performance_name = 'extra_cost_core_hours-performance-' + specifier_str
#     extra_cost_core_hours_even_name = 'extra_cost_core_hours-even-' + specifier_str
#     cost_performance_name = 'cost-performance-' + specifier_str
#     cost_even_name = 'cost-even-' + specifier_str
#     cost_name = 'cost-' + specifier_str
#
#     slowdown_estimate_name = 'slowdown-estimate-' + specifier_str
#     cost_estimate_performance_name = 'cost-estimate-performance-' + specifier_str
#     cost_estimate_even_name = 'cost-estimate-even-' + specifier_str
#
#     cost_ratio_performance_name = 'cost-ratio-performance-' + specifier_str
#     cost_ratio_performance_online_offline_name = 'cost-ratio-performance-' + specifier_str + '-offline'
#
#     cost_estimate_ratio_performance_name = 'cost-estimate-ratio-performance-' + specifier_str
#
#
#     # cost_ratio_even_name = 'cost-ratio-even-' + specifier_str
#
#     job_tuples = []
#     for jobid, job in jobs_metrics_current.iteritems():
#         temp_tuple = (job['queued_time'], jobid)
#         job_tuples.append(temp_tuple)
#
#     job_tuples.sort()
#
#     for job_queued_time, jobid in job_tuples:
#         current_job = jobs_metrics_current[jobid]
#         current_job_baseline = jobs_metrics_baseline[jobid]
#
#     # for jobid, job in jobs_metrics_current.iteritems():
#     #     if job['trimmed'] == False:
#     #         continue
#         if current_job['job_type'] != 'rt':
#             continue
#
#         window_end = current_job['queued_time']
#         window_start = window_end - time_window.total_seconds()
#
#         # get all slowdowns for rt jobs that finished in the last 24 hours
#         recent_finished_jobs = get_recent_jobs(jobs_metrics_current, 'end_time', window_start, window_end)
#         recent_rt_slowdowns =  [job['slowdown'] for jobid, job in recent_finished_jobs.iteritems() if job['job_type'] == 'rt']
#
#         # get all slowdowns for jobs from the baseline that finished in the last 24 hours
#         # recent_finished_jobs_baseline = get_recent_jobs(jobs_metrics_baseline, 'end_time', window_start, window_end)
#         # recent_baseline_slowdowns =  [job['slowdown'] for jobid, job in recent_finished_jobs_baseline.iteritems()]
#
#         # get baseline slowdown values for rt jobs that finished in the last 24 hours
#         recent_finished_jobs_ids = [jobid for jobid, job in recent_finished_jobs.iteritems()]
#         recent_baseline_slowdowns =  [jobs_metrics_baseline[tmp_id]['slowdown'] for tmp_id in recent_finished_jobs_ids]
#
#         if len(recent_rt_slowdowns) > 0:
#             average_recent_rt_slowdown = float(sum(recent_rt_slowdowns)) / len(recent_rt_slowdowns)
#         else:
#             average_recent_rt_slowdown = 1.0
#
#         if len(recent_baseline_slowdowns) > 0:
#             average_recent_baseline_slowdown = float(sum(recent_baseline_slowdowns)) / len(recent_baseline_slowdowns)
#         else:
#             average_recent_baseline_slowdown = 1.0
#
#         current_job[slowdown_estimate_name] = average_recent_rt_slowdown
#
#         job_core_hours = current_job_baseline['log_run_time'] * current_job['nodes'] / 60.0
#         performance_job_core_hours = average_recent_baseline_slowdown/average_recent_rt_slowdown * job_core_hours / 60.0
#
#         if performance_job_core_hours < 0:
#             current_job[extra_cost_core_hours_performance_name] = 0
#         else:
#             current_job[extra_cost_core_hours_performance_name] = performance_job_core_hours
#
#         # get performance_job_core_hours for all rt jobs that queued in the last 24 hours
#         recent_queue_jobs = get_recent_jobs(jobs_metrics_current, 'queued_time', window_start, window_end)
#
#         recent_performance_core_hours = sum([job[extra_cost_core_hours_performance_name] for jobid, job in
#                                                  recent_queue_jobs.iteritems() if job['job_type'] == 'rt'])
#
#         # get extra reimbursement hours for all batch jobs that finished in the last 24 hours
#         recent_reimbursement_core_hours = 0
#         for jobid, job in recent_finished_jobs.iteritems():
#             if job['job_type'] == 'batch':
#                 job_baseline = jobs_metrics_baseline[jobid]
#
#                 core_hour_difference = (job['turnaround_time'] - job_baseline['turnaround_time']) * job['nodes'] / 60.0
#                 core_hour_difference_threshold = job_baseline['turnaround_time'] * job[
#                     'nodes'] / 60.0 * reimbursement_threshold
#                 if core_hour_difference > core_hour_difference_threshold:
#                     core_hour_difference -= core_hour_difference_threshold
#                     recent_reimbursement_core_hours += core_hour_difference
#
#         extra_core_hours = performance_job_core_hours / recent_performance_core_hours * recent_reimbursement_core_hours
#
#         current_job[cost_estimate_performance_name] = current_job['cost_original'] + extra_core_hours
#
#         # if the job had worse performance than we expected - reimburse it based on its quote price
#         if current_job['slowdown'] > current_job[slowdown_estimate_name] * (1.0 + reimbursement_threshold):
#             # current_job[cost_estimate_performance_name]
#             #
#             # job_core_hours = job_baseline['log_run_time'] * job['nodes'] / 60.0
#             #
#             # core_hour_original = job_baseline['log_run_time'] * job_baseline['slowdown'] * job['nodes'] / 60.0
#             # core_hour_sim = job_baseline['log_run_time'] * current_job['slowdown'] * job['nodes'] / 60.0
#             # core_hour_sim - core_hour_original
#             #
#             # slope = (current_job[cost_estimate_performance_name] - current_job['cost_original']) / \
#             #         (current_job['slowdown'] - current_job[slowdown_estimate_name])
#
#             core_hour_difference = current_job_baseline['log_run_time'] * \
#                                    (current_job['slowdown'] - current_job[slowdown_estimate_name] * (1.0 + reimbursement_threshold)) * job['nodes'] / 60.0
#
#             current_job[cost_performance_name] = current_job[cost_estimate_performance_name] - core_hour_difference
#
#         else:
#             current_job[cost_performance_name] = current_job[cost_estimate_performance_name]
#
#         # if current_log <
#         # current_job[cost_performance_name] = current_job['cost_original'] + extra_core_hours
#
#
#         cost_performance_name_offline = 'cost-performance-' + str(reimbursement_threshold) + 'slack' + '-offline'
#
#         current_job[cost_ratio_performance_name] = current_job[cost_performance_name] / current_job['cost_original']
#
#         if cost_performance_name_offline in current_job:
#             offline_cost = current_job[cost_performance_name_offline]
#         else:
#             offline_cost = current_job['cost-' + str(reimbursement_threshold) + 'slack' + '-offline']
#
#         current_job[cost_ratio_performance_online_offline_name] = current_job[cost_performance_name] / offline_cost
#         current_job[cost_estimate_ratio_performance_name] = current_job[cost_performance_name] / current_job[cost_estimate_performance_name]
#         # if current_job[cost_ratio_performance_online_offline_name] < 0:
#         #     pass


def get_data_dicts(pickle_dict, metadata_tuples, extra_filters):
    tmp_data_dicts = []

    if type(metadata_tuples) == dict:
        metadata_tuples = tuple(sorted(metadata_tuples.items()))
    if type(extra_filters) == dict:
        extra_filters = tuple(sorted(extra_filters.items()))

    for data_dict_key, data_dict in pickle_dict.iteritems():
        passed_filters = True

        for current_tuple in metadata_tuples:
            if current_tuple not in data_dict_key:
                passed_filters = False
                break
        for current_tuple in extra_filters:
            if current_tuple not in data_dict_key:
                passed_filters = False
                break
        if passed_filters is True:
            tmp_data_dicts.append(data_dict)
    return tmp_data_dicts


def get_data_dict_verified(pickle_dict, checkpoint_heuristic_to_graph, extra_filters):
    temp_data_dict = get_data_dicts(pickle_dict, checkpoint_heuristic_to_graph, extra_filters)

    try:
        assert len(temp_data_dict) == 1
    except:
        print(len(temp_data_dict))
        print(temp_data_dict)
        print(checkpoint_heuristic_to_graph)
        print(extra_filters)
        print('assertion error')
        exit()
    # temp_data_dict = temp_data_dict[0]
    return temp_data_dict[0]


def load_pickle_data(pickle_filename):
    try:
        pickle_data = pickle.load(open(pickle_filename, "rb"))
    except:
        print('Error parsing pickle file - ', pickle_filename)
        exit(-1)

    # experiment_metrics = pickle_data['experiment_metrics']
    # jobs_metrics = pickle_data['jobs_metrics']
    # trimmed_utilization_records = pickle_data['trimmed_utilization_records']
    # utilization_records = pickle_data['utilization_records']

    return pickle_data

def make_pickle_name(pickle_filename):
    # test = '5%-all-v2p_app_sam_v1-walltime-5%_overhead-mira.xml-wk1.log-trial0.pickle'
    log_filename_parts = pickle_filename.split('-')

    temp_log_dict = {}
    temp_log_dict['rt_percent'] = log_filename_parts.pop(0)
    temp_log_dict['rt_job_category'] = log_filename_parts.pop(0)
    temp_log_dict['checkpoint_heuristic'] = log_filename_parts.pop(0)

    if 'sam' in temp_log_dict['checkpoint_heuristic']:
        temp_log_dict['runtime_estimator'] = log_filename_parts.pop(0)

    if 'v2p_app_sam_v1' in temp_log_dict['checkpoint_heuristic']:
        temp_log_dict['checkpoint_overhead'] = log_filename_parts.pop(0)

    temp_log_dict['simulator_file'] = log_filename_parts.pop(0)

    temp_log_dict['trial_number'] = log_filename_parts.pop(-1).split('.')[0]

    temp_log_dict['log_file'] = log_filename_parts.pop(0)

    return temp_log_dict


if __name__== "__main__":
    start_time = datetime.datetime.now()
    print('start time: ' + str(start_time))

    main()

    print('\ndone!\n')

    end_time = datetime.datetime.now()
    print('end time: ' + str(end_time))
    print('run time: ' + str(end_time - start_time))