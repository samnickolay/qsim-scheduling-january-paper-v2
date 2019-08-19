
def compute_online_pricing_metrics(pickle_dict):

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


    return 0

#
# Measure how often I meet deadline,
# Measure how far I exceed deadline
# Average offline vs online cost for jobs – box plot
# Box plot for quoted price/slowdown
# Box plot for resulting price/slowdown
# % of RTJs that became batch jobs since we couldn’t meet their slowdown/pricing constraints
# Need to all the above metrics by job category

from 'make-pricing-graphs.py' import rt_percents, number_trials, current_log, get_data_dict_verified, \
    checkpoint_heuristics_to_compare, checkpoint_heuristic_baseline,


def compute_percentage_meet_slowdown_deadline(pickle_dict):
    jobs_metrics_current = current_data_dict['jobs_metrics']


    for rt_percent in rt_percents:
        for trial_number in range(number_trials):
            extra_filters = {'rt_percent': rt_percent, 'trial_number': 'trial' + str(trial_number), 'log_file': current_log}
            baseline_data_dict = get_data_dict_verified(pickle_dict, checkpoint_heuristic_baseline, extra_filters)

            for checkpoint_heuristic in checkpoint_heuristics_to_compare:
                current_data_dict = get_data_dict_verified(pickle_dict, checkpoint_heuristic, extra_filters)

                jobs_metrics_current = current_data_dict['jobs_metrics']




    met_deadline_count = 0.0
    total_count = 0.0
    for jobid, job in jobs_metrics_current.iteritems():
        if job['job_type'] == 'rt':




