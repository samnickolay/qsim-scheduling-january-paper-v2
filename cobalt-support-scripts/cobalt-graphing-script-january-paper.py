# cobalt-graphing-script.py


import matplotlib.pyplot as plt
from matplotlib.pyplot import setp, plot

import os
import numpy as np
import json

# log_directory = '/home/samnickolay/slowdown_results/'


trimmed_global = "trimmed" # "all"
print('\nTrimmed global variable value is: ' + trimmed_global + '\n')


# results_folder = "results_8-25-19_wk1_APS_RTJs"


results_base_directory = "results_9-13_10-trials/"
# results_directory = results_base_directory +"cea_curie_original"
# results_directory=results_base_directory +"cea_curie_90min_restriction"
# results_directory=results_base_directory +"cea_curie_APS_RTJS"
# results_directory=results_base_directory +"mira_wk1_original"
# results_directory=results_base_directory +"mira_wk1_90min_restriction"
results_directory=results_base_directory +"mira_wk1_APS_RTJS"


global_spreadsheet_metric = 'average'
# global_spreadsheet_metric = 'median'

# make_plots_bool = True
make_plots_bool = False

make_spreadsheet_bool = True
# make_spreadsheet_bool = False

only_low_walltime_runtime_ratio_jobs = True
# only_low_walltime_runtime_ratio_jobs = False

current_data = None

if 'cea_curie' in results_directory:
    current_data = 'cea_curie'
elif 'mira_wk1' in results_directory:
    current_data = 'wk1'

# current_data = 'cea_curie'
# current_data = 'wk1'

# current_data = 'wk2'
# current_data = 'wk3'

current_data2 = None

# current_data = 'wk2'
# current_data2 = 'wk3'


results_folder = results_directory
log_directory = '/home/samnickolay/' + results_folder + '/metric_results/'



# log_directory = '/home/samnickolay/' + results_folder + '/slowdown_results/'

# log_directory = '/home/samnickolay/slowdown_results-lowSD-10trials/'

# log_directory = '/home/samnickolay/old results - 8-28/slowdown_results-lowSD-10trials/'

# current_data = 'wk1-lowSD'


# rt_percents = ['5%', '10%', '15%', '20%']
rt_percents = [5, 10, 15, 20]


# checkpoint_heuristics = ['baseline', 'highpQ', 'v1', 'v2', 'v2_sam_v1', 'v2p', 'v2p_sam_v1', 'v2p_app', 'v2p_app_sam_v1']
checkpoint_heuristics = ['baseline', 'v2_sam_v1', 'v2p_app_sam_v1']

runtime_estimators = ['walltime', 'actual', 'predicted']
# checkp_overhead_percents = ['5', '10', '20']
# checkp_overhead_percents = ['5', '10']


extra_metrics = ['makespan', 'system_utilization', 'productive_utilization', 'overhead_utilization' ]

# metrics = [
# # 'rtj_aver_slowdown',
# # 'rest_aver_slowdown',
# # 'rtj_avg_turnaround_time',
# # 'batchj_avg_turnaround_time',
# 'all_slowdown_rt',
# 'all_slowdown_batch',
# 'all_turnaround_time_rt',
# 'all_turnaround_time_batch',
# 'narrow_short_slowdown_rt',
# 'narrow_short_slowdown_batch',
# 'narrow_short_turnaround_time_rt',
# 'narrow_short_turnaround_time_batch',
# 'narrow_long_slowdown_rt',
# 'narrow_long_slowdown_batch',
# 'narrow_long_turnaround_time_rt',
# 'narrow_long_turnaround_time_batch',
# 'wide_short_slowdown_rt',
# 'wide_short_slowdown_batch',
# 'wide_short_turnaround_time_rt',
# 'wide_short_turnaround_time_batch',
# 'wide_long_slowdown_rt',
# 'wide_long_slowdown_batch',
# 'wide_long_turnaround_time_rt',
# 'wide_long_turnaround_time_batch']


job_sizes = ['', 'narrow_short_', 'narrow_long_', 'wide_short_', 'wide_long_']
job_types = ['rt', 'batch']
job_metrics = ['slowdown_', 'turnaround_time_', 'count_']

metrics = extra_metrics[:]
for job_size in job_sizes:
    for job_metric in job_metrics:
        for job_type in job_types:
            metrics.append(job_size + job_metric + job_type)

print(metrics)

print('\n'+ results_directory)

# data_dicts = []


def main():

    # # current_data = 'cea_curie'
    # current_data = 'wk1'
    # # current_data = 'wk2'
    # # current_data = 'wk3'
    #
    # current_data2 = None
    #
    # # current_data = 'wk2'
    # # current_data2 = 'wk3'

    data_dicts = parse_logs(current_data)


    if make_plots_bool is True:
        if current_data2 is not None:
            data_dicts2 = parse_logs(current_data2)
            data_name = current_data + '-' + current_data2
            make_graphs(data_name, data_dicts, data_dicts2)
        else:
            make_graphs(current_data, data_dicts)
        
    # spreadsheet_metrics = ['slowdown_rt', 'slowdown_batch', 'turnaround_time_rt', 'turnaround_time_batch', 'count_rt', 'count_batch']

    if make_spreadsheet_bool is True:
        spreadsheet_metrics = ['slowdown_runtime_rt', 'slowdown_runtime_batch',
                               'turnaround_time_rt', 'turnaround_time_batch', 'count_rt', 'count_batch']

        if only_low_walltime_runtime_ratio_jobs is True:
            spreadsheet_metrics = [metric + '_low_walltime_runtime_ratio' for metric in spreadsheet_metrics]



        checkpoint_heuristics_to_spreadsheet = [{'checkpoint_heuristic': 'baseline'},
                                      {'checkpoint_heuristic': 'v2p_app_sam_v1', 'checkpoint_overhead': 5.0, 'runtime_estimator': 'walltime' },
                                      {'checkpoint_heuristic': 'v2p_app_sam_v1', 'checkpoint_overhead': 10.0, 'runtime_estimator': 'walltime' },
                                      {'checkpoint_heuristic': 'v2p_app_sam_v1', 'checkpoint_overhead': 20.0, 'runtime_estimator': 'walltime' },
                                      {'checkpoint_heuristic': 'v2_sam_v1', 'runtime_estimator': 'walltime' }]

        # checkpoint_heuristics_to_spreadsheet = [{'checkpoint_heuristic': 'baseline'}, {'checkpoint_heuristic': 'highpQ'}, {'checkpoint_heuristic': 'v2'},
        #                                         {'checkpoint_heuristic': 'v2p_app'}]

        # {'checkpoint_heuristic': 'v2p'}, {'checkpoint_heuristic': 'v2p_app'}]

        make_spreadsheet(data_dicts, checkpoint_heuristics_to_spreadsheet, spreadsheet_metrics)


def make_graphs(current_data, data_dicts, data_dicts2=None):
    checkpoint_heuristics_to_graph = [{'checkpoint_heuristic': 'baseline'},
                                      {'checkpoint_heuristic': 'v2p_app_sam_v1', 'checkpoint_overhead': 5.0,
                                       'runtime_estimator': 'walltime'},
                                      {'checkpoint_heuristic': 'v2p_app_sam_v1', 'checkpoint_overhead': 10.0,
                                       'runtime_estimator': 'walltime'},
                                      # {'checkpoint_heuristic': 'v2p_app_sam_v1', 'checkpoint_overhead': '20%_overhead',
                                      #  'runtime_estimator': 'walltime'},
                                      {'checkpoint_heuristic': 'v2_sam_v1', 'runtime_estimator': 'walltime'}]

    checkpoint_heuristics_to_graph_names = ['baseline', 'SYS-APP-5%', 'SYS-APP-10%', 'JIT-CKPT']
    # checkpoint_heuristics_to_graph_names = ['baseline', 'SYS-APP-5%', 'SYS-APP-10%', 'SYS-APP-20%', 'JIT-CKPT']
    # make_graphs(checkpoint_heuristics_to_graph, checkpoint_heuristics_to_graph_names)

    for job_size in job_sizes:
        output_folder = '/home/samnickolay/plots/' + results_folder + '/'
        plot_type = 'average'
        output_file_name = output_folder + plot_type + '/' + current_data + '-' + job_size.strip('_') + '.png'
        make_graph(data_dicts, checkpoint_heuristics_to_graph, checkpoint_heuristics_to_graph_names, output_file_name, job_size,
                   plot_type, log_y=False, max_min_dots=False, data_dicts2=data_dicts2)

        plot_type = 'median'
        output_file_name = output_folder + plot_type + '/' + current_data + '-' + job_size.strip('_') + '.png'
        make_graph(data_dicts, checkpoint_heuristics_to_graph, checkpoint_heuristics_to_graph_names, output_file_name, job_size,
                   plot_type, log_y=False, max_min_dots=False, data_dicts2=data_dicts2)


        plot_type = 'box'
        output_file_name = output_folder + plot_type + '/' + current_data + '-' + job_size.strip('_') + '.png'
        make_graph(data_dicts, checkpoint_heuristics_to_graph, checkpoint_heuristics_to_graph_names, output_file_name, job_size,
                   plot_type, log_y=False, max_min_dots=False, data_dicts2=data_dicts2)

        plot_type = 'box'
        output_file_name = output_folder + plot_type + '-log_y/' + current_data + '-' + job_size.strip('_') + '.png'
        make_graph(data_dicts, checkpoint_heuristics_to_graph, checkpoint_heuristics_to_graph_names, output_file_name, job_size,
                   plot_type, log_y=True, max_min_dots=False, data_dicts2=data_dicts2)

        plot_type = 'box'
        output_file_name = output_folder + plot_type + '-max_min_dots/' + current_data + '-' + job_size.strip('_') + '.png'
        make_graph(data_dicts, checkpoint_heuristics_to_graph, checkpoint_heuristics_to_graph_names, output_file_name, job_size,
                   plot_type, log_y=False, max_min_dots=True, data_dicts2=data_dicts2)

        plot_type = 'box'
        output_file_name = output_folder + plot_type + '-log_y-max_min_dots/' + current_data + '-' + job_size.strip('_') + '.png'
        make_graph(data_dicts, checkpoint_heuristics_to_graph, checkpoint_heuristics_to_graph_names, output_file_name, job_size,
                   plot_type, log_y=True, max_min_dots=True, data_dicts2=data_dicts2)

        #######################333
        # output_folder = '/home/samnickolay/plots/'
        # plot_type = 'average'
        # output_file_name = output_folder + plot_type + '/' + current_data + '-' + job_size.strip('_') + '.png'
        # make_graph(data_dicts, checkpoint_heuristics_to_graph, checkpoint_heuristics_to_graph_names, output_file_name, job_size,
        #            plot_type, log_y=False, max_min_dots=False, data_dicts2=data_dicts2)


def parse_logs(current_data):
    # global data_dicts
    data_dicts = []
    for log_filename in os.listdir(log_directory):
        if log_filename.endswith(".log.json") and current_data in log_filename:

            with open(log_directory + log_filename, 'r') as fp:
                temp_log_dict = json.load(fp)
            data_dicts.append(temp_log_dict)

        else:
            continue

    return data_dicts




def get_data_dicts(data_dicts, metadata_dict, extra_filters):
    tmp_data_dicts = []
    for data_dict in data_dicts:
        passed_filters = True
        for metadata_key, metadata_value in metadata_dict.iteritems():
            if metadata_key not in data_dict or data_dict[metadata_key] != metadata_value:
                passed_filters = False
                break
        for metadata_key, metadata_value in extra_filters.iteritems():
            if metadata_key not in data_dict or data_dict[metadata_key] != metadata_value:
                passed_filters = False 
                break
        if passed_filters is True:
            tmp_data_dicts.append(data_dict)
    return tmp_data_dicts



def get_data_dict_values(data_dicts, metric, trimmed, checkpoint_heuristic_to_graph, rt_percent):
    tmp_data_dict = get_data_dict_verified(data_dicts, checkpoint_heuristic_to_graph, rt_percent)
    if trimmed == 'all':
        current_values = tmp_data_dict['job_metrics_all'][metric]
    elif trimmed == 'trimmed':
        current_values = tmp_data_dict['job_metrics_trimmed'][metric]
    else:
        print('invalid argument for trimmed in get_data_dict_values (options are "all" or "trimmed")')

    return current_values



def get_data_dict_verified(data_dicts, checkpoint_heuristic_to_graph, rt_percent):
    extra_filters = {'rt_percent': rt_percent}
    temp_data_dict = get_data_dicts(data_dicts, checkpoint_heuristic_to_graph, extra_filters)

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


# make graph
def make_graph(data_dicts, checkpoint_heuristics_to_graph, checkpoint_heuristics_to_graph_names, output_file_name, job_size,
               plot_type, log_y, max_min_dots, data_dicts2=None):

    # plotting_metrics = ['slowdown_rt', 'slowdown_batch', 'turnaround_time_rt', 'turnaround_time_batch']
    # plotting_metrics = ['slowdown_walltime_rt', 'slowdown_walltime_batch', 'turnaround_time_rt',
    #                     'turnaround_time_batch']
    # plotting_metrics = ['slowdown_runtime_rt', 'slowdown_runtime_batch', 'slowdown_runtime_rt',
    #                     'slowdown_runtime_batch']

    plotting_metrics = ['slowdown_runtime_rt', 'slowdown_runtime_batch', 'turnaround_time_rt',
                        'turnaround_time_batch']

    plotting_metrics_names = ['(a) Real-time jobs - slowdown', '(b) Batch jobs - slowdown',
                              '(c) Real-time jobs - turnaround time', '(d) Batch jobs - turnaround time',]

    # if data_dicts2 is None:
    #     plotting_metrics = ['slowdown_rt', 'slowdown_batch', 'turnaround_time_rt', 'turnaround_time_batch']
    #     plotting_metrics_names = ['(a) Real-time jobs - slowdown', '(b) Batch jobs - slowdown',
    #                               '(c) Real-time jobs - turnaround time', '(d) Batch jobs - turnaround time', ]
    #
    # else:
    #     plotting_metrics = ['slowdown_rt', 'slowdown_batch', 'slowdown_rt', 'slowdown_batch',]
    #     plotting_metrics_names = ['(a) Wk-b - Real-time jobs - slowdown', '(b) Wk-b - Batch jobs - slowdown',
    #                               '(c) Wk-c - Real-time jobs - slowdown', '(d) Wk-c - Batch jobs - slowdown', ]

    colors = ['b', 'r', 'g', 'm', 'y']

    # plot_type = 'average'
    # plot_type = 'box'
    if plot_type not in ['average', 'median', 'box']:
        print('invalid plot type')
        print(plot_type)
        exit(-1)

    print('making figure: ' + output_file_name)

    # for job_size in job_sizes:
        # make a figure

    f, axarr = plt.subplots(2, 2, figsize=(9, 6))

    for idx, plotting_metric in enumerate(plotting_metrics):

        if data_dicts2 is not None and idx >= 2:
            current_data_dicts = data_dicts2
        else:
            current_data_dicts = data_dicts

        current_metric = job_size + plotting_metric
        # print('current metric: ' + current_metric)

        axis0 = int(idx / 2)
        axis1 = idx % 2

        current_ax = axarr[axis0, axis1]

        rects_list = []

        offset = 0.0
        color_idx = 0

        x_axis_numbers = np.arange(len(rt_percents))

        #####################################################33

        if plot_type == 'average' or plot_type=='median':
            width = 0.18
            for checkpoint_heuristic_to_graph in checkpoint_heuristics_to_graph:
                current_values = []
                for rt_percent in rt_percents:
                    # tmp_data_dict = get_data_dict_verified(current_data_dicts, checkpoint_heuristic_to_graph, rt_percent)
                    # values_list = tmp_data_dict['metrics'][current_metric]

                    values_list = get_data_dict_values(current_data_dicts, current_metric, trimmed_global,
                                                       checkpoint_heuristic_to_graph, rt_percent)

                    if plot_type == 'average':
                        if len(values_list) > 0:
                            current_value = float(sum(values_list)) / len(values_list)
                        else:
                            current_value = 0
                    elif plot_type == 'median':
                        if len(values_list) > 0:
                            current_value = np.median(values_list)
                        else:
                            current_value = 0

                    current_values.append(current_value)

                current_rects = current_ax.bar(x_axis_numbers+offset, current_values, width=width, color=colors[color_idx]) #, width, color='r', yerr=men_std)
                rects_list.append(current_rects)

                offset += width
                color_idx += 1

            # add some text for labels, title and axes ticks
            if 'slowdown' in current_metric:
                current_ax.set_ylabel('Avg Slow Down (x times)', fontweight='bold')
            else:
                current_ax.set_ylabel('Avg Turnaround Time (min)', fontweight='bold')
            current_ax.set_xlabel('Realtime Job Percentage (%)', fontweight='bold')
            current_ax.set_xticklabels(rt_percents)
            current_ax.set_xticks(x_axis_numbers + width * 2)

            if log_y is True:
                current_ax.set_yscale('log')

                from matplotlib.ticker import ScalarFormatter
                current_ax.yaxis.set_major_formatter(ScalarFormatter())

            f.legend(rects_list, checkpoint_heuristics_to_graph_names, loc=(0.12, 0.94), ncol=5)

        ############################
        elif plot_type == 'box':
            count = -1
            width = 0.6
            for rt_percent in rt_percents:
                count += 1
                current_values = []
                for checkpoint_heuristic_to_graph in checkpoint_heuristics_to_graph:
                    # tmp_data_dict = get_data_dict_verified(current_data_dicts, checkpoint_heuristic_to_graph, rt_percent)
                    # values_list = tmp_data_dict['metrics'][current_metric]
                    values_list = get_data_dict_values(current_data_dicts, current_metric, trimmed_global,
                                                       checkpoint_heuristic_to_graph, rt_percent)
                    if len(values_list) > 0:
                        current_values.append(values_list)
                    else:
                        current_values.append([])


                heuristic_numbers = np.arange(len(checkpoint_heuristics_to_graph))
                current_positions = heuristic_numbers + (count * (len(heuristic_numbers) + 1)) + 1
                meanpointprops = dict(marker='o', markersize=4)
                # meanpointprops = {}

                bp = current_ax.boxplot(current_values, positions = current_positions, widths = width,
                                        whis=[5, 95], showfliers=False, showmeans=True, meanprops=meanpointprops)

                if max_min_dots is True:
                    for i in heuristic_numbers:
                        if len(current_values[i])> 0:
                            min_value = min(current_values[i])
                            max_value = max(current_values[i])
                            size = 1.5
                            current_ax.plot(current_positions[i], min_value, colors[i] + 'o', markersize=size)
                            current_ax.plot(current_positions[i], max_value, colors[i] + 'o', markersize=size)

                for i in heuristic_numbers:
                    idx1 = i
                    idx2 = 2*i
                    idx3 = 2*i+1
                    setp(bp['boxes'][idx1], color=colors[i])
                    setp(bp['medians'][idx1], color=colors[i])
                    setp(bp['means'][idx1], markeredgecolor=colors[i])
                    setp(bp['means'][idx1], markerfacecolor='w')
                    setp(bp['caps'][idx2], color=colors[i])
                    setp(bp['caps'][idx3], color=colors[i])
                    setp(bp['whiskers'][idx2], color=colors[i])
                    setp(bp['whiskers'][idx3], color=colors[i])
                    # setp(bp['whiskers'][idx2], alpha=0.5)
                    # setp(bp['whiskers'][idx3], alpha=0.5)

                    # setp(bp['fliers'][idx2], color=colors[i])
                    # setp(bp['fliers'][idx3], color=colors[i])

                color_idx += 1

            # add some text for labels, title and axes ticks
            if 'slowdown' in current_metric:
                current_ax.set_ylabel('Slow Down (x times)', fontweight='bold')
            else:
                current_ax.set_ylabel('Turnaround Time (min)', fontweight='bold')
            current_ax.set_xlabel('Realtime Job Percentage (%)', fontweight='bold')
            current_ax.set_xticklabels(rt_percents)
            # current_ax.set_xticks(x_axis_numbers*width*(len(checkpoint_heuristics_to_graph)+1) + 1)
            current_ax.set_xticks([0.0, 3.0, 8.0, 13.0, 18.0])
            current_ax.set_xticks(x_axis_numbers*(len(checkpoint_heuristics_to_graph)+1) + 3.0)

            if log_y is True:
                current_ax.set_yscale('log')

                from matplotlib.ticker import ScalarFormatter
                current_ax.yaxis.set_major_formatter(ScalarFormatter())

            lines = []
            for color in colors:
                l, = plot([0.5, 0.5], color + '-')
                lines.append(l)
            f.legend(lines, checkpoint_heuristics_to_graph_names, loc=(0.20, 0.94), ncol=5)

            for line in lines:
                line.set_visible(False)

        ########################################################3

        current_title = current_ax.set_title(plotting_metrics_names[idx], fontsize=15)

        current_title.set_position([.48, -0.45])

    # f.legend(rects_list, checkpoint_heuristics_to_graph_names, loc=(0.12, 0.94), ncol=5)

    # f.subplots_adjust(top=0.92, bottom=0.5, left=0.5, right=0.95, hspace=0.75, wspace=0.75)
    plt.subplots_adjust(top=0.92, bottom=0.15, left=0.08, right=0.98, hspace=0.55, wspace=0.25)

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


def get_row_values(data_dicts, metric, checkpoint_heuristics_to_use, current_spreadsheet_metric):
    current_line = ''
    for rt_percent in rt_percents:
        for checkpoint_heuristic_to_use in checkpoint_heuristics_to_use:
            # extra_filters = {'rt_percent': rt_percent}
            # temp_data_dict = get_data_dicts(data_dicts, checkpoint_heuristic_to_use, extra_filters)
            #
            # try:
            #     assert len(temp_data_dict) == 1
            # except:
            #     pass
            # temp_data_dict = temp_data_dict[0]

            # current_data_list = temp_data_dict['metrics'][metric]

            current_data_list = get_data_dict_values(data_dicts, metric, trimmed_global,
                                                     checkpoint_heuristic_to_use, rt_percent)

            boxplot_values = False

            if len(current_data_list) > 0:
                # avg_value = sum(current_data_list)/ float(len(current_data_list))
                if current_spreadsheet_metric == "average":
                    value = sum(current_data_list) / float(len(current_data_list))
                elif current_spreadsheet_metric == "median":
                    value = np.median(current_data_list)
                else:
                    exit(-1)

                value_str = str(value)

                if boxplot_values is True:

                    median = np.median(current_data_list)
                    upper_quartile = np.percentile(current_data_list, 75)
                    lower_quartile = np.percentile(current_data_list, 25)
                    value_str = str(avg_value) + ', ' + str(median) + ', ' + str(lower_quartile) + ', ' + str(upper_quartile)

                # iqr = upper_quartile - lower_quartile
                # upper_whisker = data[data <= upper_quartile + 1.5 * iqr].max()
                # lower_whisker = data[data >= lower_quartile - 1.5 * iqr].min()

            else:
                avg_value = -1
                value_str = str(avg_value)

                if boxplot_values is True:
                    value_str = str(avg_value) + ', ' + str(avg_value) + ', ' + str(avg_value) + ', ' + str(avg_value)

            current_line += value_str + ', '

        current_line += ', '
    return current_line


# def get_row_data_lists(metric, checkpoint_heuristics_to_use):
#     data_lists = []
#     for rt_percent in rt_percents:
#         for checkpoint_heuristic_to_graph in checkpoint_heuristics_to_use:
#             extra_filters = {'rt_percent': rt_percent}
#             temp_data_dict = get_data_dicts(checkpoint_heuristics_to_use, extra_filters)

#             assert len(temp_data_dict) == 1
#             temp_data_dict = temp_data_dict[0]

#             current_value = temp_data_dict['metrics'][metric]
#             data_lists.append(current_value)
#     return data_lists


# def make_average_string(data_lists):
#     output_str = ''
#     for data_list in data_lists:
#         if len(data_list) > 0:
#             avg_value = sum(data_list)/ float(len(data_list))
#         else:
#             avg_value = -1
#         output_str += str(avg_value) + ', '
#     return output_str


# make spreadsheet
def make_spreadsheet(data_dicts, checkpoint_heuristics_to_spreadsheet, spreadsheet_metrics):

    spreadsheet_lines = []
    global global_spreadsheet_metric

    for trimmed in ['_trimmed']:
    # for trimmed in ['', '_trimmed']:

        spreadsheet_lines = []

        for extra_metric in extra_metrics:
            current_line = get_row_values(data_dicts, extra_metric, checkpoint_heuristics_to_spreadsheet, global_spreadsheet_metric)
            spreadsheet_lines.append(current_line)

        for job_size in job_sizes:
            spreadsheet_lines.append('')
            for spreadsheet_metric in spreadsheet_metrics:
                current_metric = job_size + spreadsheet_metric

                current_line = get_row_values(data_dicts, current_metric, checkpoint_heuristics_to_spreadsheet, global_spreadsheet_metric)
                spreadsheet_lines.append(current_line)

                # current_data_lists = get_row_data_lists(get_row_data_lists)
                # current_line = make_average_string(current_data_lists)
                # spreadsheet_lines.append(current_line)

        print('\n' + trimmed + '\n')
        for spreadsheet_line in spreadsheet_lines:
            print(spreadsheet_line)
        print('\n\n')
        print('Spreadsheet metric == ' + global_spreadsheet_metric )


        # metrics = [
# # 'rtj_aver_slowdown',
# # 'rest_aver_slowdown',
# # 'rtj_avg_turnaround_time',
# # 'batchj_avg_turnaround_time',
# 'all_slowdown_rt',
# 'all_slowdown_batch',
# 'all_turnaround_time_rt',
# 'all_turnaround_time_batch',
# 'narrow_short_slowdown_rt',
# 'narrow_short_slowdown_batch',
# 'narrow_short_turnaround_time_rt',
# 'narrow_short_turnaround_time_batch',
# 'narrow_long_slowdown_rt',
# 'narrow_long_slowdown_batch',
# 'narrow_long_turnaround_time_rt',
# 'narrow_long_turnaround_time_batch',
# 'wide_short_slowdown_rt',
# 'wide_short_slowdown_batch',
# 'wide_short_turnaround_time_rt',
# 'wide_short_turnaround_time_batch',
# 'wide_long_slowdown_rt',
# 'wide_long_slowdown_batch',
# 'wide_long_turnaround_time_rt',
# 'wide_long_turnaround_time_batch']




# axarr[0, 0].plot(x, y)
# axarr[0, 0].set_title('Axis [0,0]')
# axarr[0, 1].scatter(x, y)
# axarr[0, 1].set_title('Axis [0,1]')
# axarr[1, 0].plot(x, y ** 2)
# axarr[1, 0].set_title('Axis [1,0]')
# axarr[1, 1].scatter(x, y ** 2)
# axarr[1, 1].set_title('Axis [1,1]')
# # Fine-tune figure; hide x ticks for top plots and y ticks for right plots
# plt.setp([a.get_xticklabels() for a in axarr[0, :]], visible=False)
# plt.setp([a.get_yticklabels() for a in axarr[:, 1]], visible=False)


# rects1 = ax.bar(ind, men_means, width, color='r', yerr=men_std)

# women_means = (25, 32, 34, 20, 25)
# women_std = (3, 5, 2, 3, 3)
# rects2 = ax.bar(ind + width, women_means, width, color='y', yerr=women_std)

# # add some text for labels, title and axes ticks
# ax.set_ylabel('Scores')
# ax.set_title('Scores by group and gender')
# ax.set_xticks(ind + width / 2)
# ax.set_xticklabels(('G1', 'G2', 'G3', 'G4', 'G5'))



if __name__== "__main__":
    main()