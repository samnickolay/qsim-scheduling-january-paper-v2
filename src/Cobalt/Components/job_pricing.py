# job_pricing.py
# code for doing the online job pricing

high_priority_queue_slowdown_percentage_threshold = 0.75

job_pricing_queue_dict = {}
high_priority_queue_dict = {}
completed_jobs_pricing_queue_dict = {}

min_queue_position = 0


def get_job_from_either_pricing_queue_dict_or_high_priority_queue_dict(jobid):
    global high_priority_queue_dict
    global job_pricing_queue_dict

    if jobid in high_priority_queue_dict:
        return get_job_from_high_priority_queue_dict(jobid)

    elif jobid in job_pricing_queue_dict:
        return get_job_from_pricing_queue_dict(jobid)
    else:
        print('Error in get_job_from_either_pricing_queue_dict_or_high_priority_queue_dict - job not in either queue_dict: ', str(jobid))
        exit(-1)


def get_job_from_pricing_queue_dict(jobid):
    for jobid_tmp, job in job_pricing_queue_dict.iteritems():
        if jobid == jobid_tmp:
            return job

    print('Error - could not find job in pricing_queue_dict')

    return None

def get_job_from_high_priority_queue_dict(jobid):
    for jobid_tmp, job in high_priority_queue_dict.iteritems():
        if jobid == jobid_tmp:
            return job

    print('Error - could not find job in get_job_from_high_priority_queue_dict')

    return None


def get_job_from_completed_jobs_pricing_queue_dict(jobid):
    for jobid_tmp, job in completed_jobs_pricing_queue_dict.iteritems():
        if jobid == jobid_tmp:
            return job

    print('Error - could not find job in completed_jobs_pricing_queue_dict')

    return None


def get_jobs_list_from_pricing_queue_dict():
    job_list = []
    for jobid_tmp, job in job_pricing_queue_dict.iteritems():
        job_list.append(job)
    return job_list


def get_jobs_list_from_high_priority_queue_dict():
    job_list = []
    for jobid_tmp, job in high_priority_queue_dict.iteritems():
        job_list.append(job)
    return job_list

def add_job_to_pricing_queue_dict(current_job):
    global job_pricing_queue_dict
    # print('Job Pricing Queue - checking that job is not already in pricing_queue_dict: ' + str(current_job.jobid))
    if current_job.jobid in job_pricing_queue_dict:
        print('Error - current_job.jobid in job_pricing_queue_dict - ' + str(current_job.jobid))
        exit(-1)
    # assert current_job.jobid not in job_pricing_queue_dict
    job_pricing_queue_dict[current_job.jobid] = current_job


def insert_batch_job_in_pricing_queue(current_job):
    # print('Job Pricing Queue - inserting batch job into pricing queue: ' + str(current_job.jobid))

    add_job_to_pricing_queue_dict(current_job)


def insert_RTJ_in_pricing_queue(current_job, all_bqsim_jobs, currently_running_jobs, now):

    # print('Job Pricing Queue - inserting realtime job into pricing queue: ' + str(current_job.jobid))

    add_job_to_pricing_queue_dict(current_job)

    # if len(get_jobs_list_from_pricing_queue_dict()) != len(all_bqsim_jobs):
    #     l = get_jobs_list_from_pricing_queue_dict()
    #     l1 = len(l)
    #     l2 = len(all_bqsim_jobs)
    #     pass

    pricing_queue_job_list = get_jobs_list_from_pricing_queue_dict()
    high_priority_queue_job_list = get_jobs_list_from_high_priority_queue_dict()

    if len(pricing_queue_job_list) + len(high_priority_queue_job_list) != len(all_bqsim_jobs):
        print('Error - len(get_jobs_list_from_pricing_queue_dict()) != len(all_bqsim_jobs)')
        print(len(pricing_queue_job_list))
        print(sorted([str(job.jobid) for job in pricing_queue_job_list]))
        print(len(high_priority_queue_job_list))
        print(sorted([str(job.jobid) for job in high_priority_queue_job_list]))
        print(len(all_bqsim_jobs))
        print(sorted([str(job.jobid) for job in all_bqsim_jobs]))
        exit(-1)
        # assert len(get_jobs_list_from_pricing_queue_dict()) == len(all_bqsim_jobs)

    compute_pricing_slowdown_quotes(current_job, currently_running_jobs, now)

    compute_max_price_max_slowdown(current_job)

    compute_queue_position(current_job, now)


def compute_pricing_slowdown_quotes(current_job, currently_running_jobs, now):
    estimates = []

    all_jobs_temp = get_jobs_list_from_pricing_queue_dict()

    all_jobs_minus_current_job = [j for j in all_jobs_temp if j != current_job]

    if len(all_jobs_minus_current_job) != len(job_pricing_queue_dict) - 1:
        print('Error - in insert_RTJ_in_pricing_queue')
        exit(-1)

    all_jobs_minus_current_job = sort_jobs_based_on_pricing_queue(all_jobs_minus_current_job)

    from bqsim import compute_slowdown
    from bqsim import TOTAL_NODES

    total_remaining_core_hours = 0.0
    total_core_hour_slowdowns = 0.0
    for currently_running_job in currently_running_jobs:

        slowdown, job_end_time, runtime = compute_slowdown(currently_running_job)
        # compute estimated remaining time for job in hours
        remaining_time = (job_end_time - now) / 3600.0
        remaining_core_hours = remaining_time * float(currently_running_job.get('nodes'))

        total_remaining_core_hours += remaining_core_hours
        total_core_hour_slowdowns += remaining_core_hours * slowdown

    if total_remaining_core_hours > 0.0:
        weighted_average_slowdown_current_jobs = total_core_hour_slowdowns / total_remaining_core_hours
    else:
        weighted_average_slowdown_current_jobs = 0.0

    # compute estimated remaining runtime for jobs in hours
    current_jobs_time = total_remaining_core_hours / TOTAL_NODES



    # compute estimated runtime for all jobs ahead of job in the queue in hours
    queue_time = 0.0

    ###
    # Current_jobs_time = time remaining for jobs currently running
    # Queue_time = time for jobs ahead of the current time in the queue
    #
    # - Base_cost = the base cost for running a RTJ (compensates for checkpointing and preemption)
    #     - Percentage of job's walltime corehours
    # - Queue_cost = cost of jobs being jumped in the queue
    #     - Current jobs walltime corehours x number of jobs being jumped in the queue
    # - Preemption_cost = cost of any jobs being preempted (maybe only if we run/preempt jobs immediately)
    # - System_load_cost/scaler = extra cost based on running/queued jobs slowdown and system utilization3
    # - Job_category_multiplier = extra cost/multiplier based on the job category
    ###

    base_cost_multiplier = 1.0
    queue_cost_multiplier = 1.0
    preemption_cost_multiplier = 1.0 # need to implement later
    system_load_cost_multiplier = 0.0 # need to implement later
    job_category_multiplier = 1.0 # need to implement later

    current_job_core_hours = float(current_job.nodes) * float(current_job.walltime) / 60.0

    # this weights the estimated waittime for jobs since our estimate uses jobs' walltimes instead of runtimes
    # the actual computed value for this ratio from log1 is 0.59
    job_runtime_walltime_ratio_estimator = 0.65

    base_cost = current_job_core_hours * base_cost_multiplier

    if len(all_jobs_minus_current_job) > 3:
        pass

    def compute_estimate(queue_position, queue_time):

        queue_cost = current_job_core_hours * (len(all_jobs_minus_current_job) - queue_position) * queue_cost_multiplier

        number_of_preemptions = 0.0
        preemption_cost = number_of_preemptions * preemption_cost_multiplier

        system_load = 1.0 # need to implement later
        # system_load = weighted_average_slowdown_current_jobs * system_load_cost_multiplier
        job_category_cost = 1.0 * job_category_multiplier

        estimated_price = (base_cost + queue_cost + preemption_cost) * system_load * job_category_cost

        estimated_waittime = (current_jobs_time + queue_time) * job_runtime_walltime_ratio_estimator
        estimated_slowdown = (float(current_job.walltime)/60.0 + estimated_waittime) / (float(current_job.walltime)/60.0)

        tmp_estimate = queue_position, estimated_slowdown, estimated_price

        estimates.append(tmp_estimate)

        if estimated_price < 100:
            pass

    for queue_position in range(len(all_jobs_minus_current_job)):
        compute_estimate(queue_position, queue_time)

        # increment queue time with the next job in the queue
        queue_time += float(all_jobs_minus_current_job[queue_position].nodes) * \
                      (float(all_jobs_minus_current_job[queue_position].walltime) / 60.0) / TOTAL_NODES

    if len(all_jobs_minus_current_job) == 0:
        compute_estimate(0, 0)

    all_estimated_slowdowns = [estimated_slowdown for queue_position, estimated_slowdown, estimated_price in estimates]
    min_estimated_slowdown = min(all_estimated_slowdowns)
    if min_estimated_slowdown > 5:
        pass

    current_job.price_slowdown_quotes = estimates
    # return estimates


def compute_max_price_max_slowdown(current_job):
    import numpy as np
    mean_slowdown, standard_deviation_slowdown = 2.0, 0.3
    max_slowdown = np.random.normal(mean_slowdown, standard_deviation_slowdown, None)


    # make the max price probability distribution dependent on the value drawn from the max slowdown distribution
    # mean_price_multiplier = 7.0 - max_slowdown
    # standard_deviation_price_multiplier = 1.5

    mean_price_multiplier, standard_deviation_price_multiplier = 5, 1.5
    max_price_multiplier = np.random.normal(mean_price_multiplier, standard_deviation_price_multiplier, None)

    # constrain the mininum values for max_slowdown and max_price_multiplier
    if max_slowdown < 1.1:
        max_slowdown = 1.1
    if max_price_multiplier < 1.5:
        max_price_multiplier = 1.5


    job_core_hours = float(current_job.walltime) / 60.0 * float(current_job.nodes)

    max_price = job_core_hours * max_price_multiplier

    current_job.max_price = max_price
    current_job.max_slowdown = max_slowdown


# Using the job's price/slowdown quotes and max_price and max_slowdown, find the best quote/queue_position
def compute_queue_position(current_job, now):

    valid_pricing_slowdown_estimates = []
    # iterate through all of the pricing quotes computed
    for pricing_slowdown_estimate in current_job.price_slowdown_quotes:
        queue_position, estimated_slowdown, estimated_price = pricing_slowdown_estimate

        # if the current quote meets the jobs price and slowdown constraints
        if estimated_price <= current_job.max_price and estimated_slowdown <= current_job.max_slowdown:
            valid_pricing_slowdown_estimates.append(pricing_slowdown_estimate)
            # if best_pricing_slowdown_estimate is None:
            #     best_pricing_slowdown_estimate = pricing_slowdown_estimate
            # # pick the cheapest quote that still meets the slowdown constraint
            # elif estimated_price < best_pricing_slowdown_estimate[2]:
            #     best_pricing_slowdown_estimate = pricing_slowdown_estimate

    # if max_price/max_slowdown are outside of the estimates given, then the job becomes a batch job
    if len(valid_pricing_slowdown_estimates) == 0:
        current_job.originally_realtime = True
        current_job.user = None
        # print('Job Pricing Queue - no valid quotes for realtime job, making it a batch job: ' + str(current_job.jobid))

        from bqsim import rtj_id
        global rtj_id

        jobid_str = str(current_job.jobid)
        if jobid_str in rtj_id:
            rtj_id.remove(jobid_str)
        else:
            pass


    # if we were able to find a quote that meets the job's constraints
    else:
        # randomly select one of the valid estimates
        from random import choice
        best_pricing_slowdown_estimate = choice(valid_pricing_slowdown_estimates)

        queue_position, estimated_slowdown, estimated_price = best_pricing_slowdown_estimate
        current_job.quoted_price = estimated_price
        current_job.quoted_slowdown = estimated_slowdown
        # compute time job exceeds quoted slowdown - used for sorting jobs in HPQ
        current_job.quoted_slowdown_time = now + (estimated_slowdown - 1.0) * float(current_job.walltime)*60.0

        # sets the jobs pricing_queue_position
        current_job.pricing_queue_position = queue_position
        current_job.original_pricing_queue_position = queue_position
        # updates the queue position for all realtime jobs in queue (any jobs behind current job are moved back one)
        all_jobs = get_jobs_list_from_pricing_queue_dict()
        for temp_job in all_jobs:
            if temp_job.pricing_queue_position >= queue_position and temp_job.user == 'realtime':
                temp_job.pricing_queue_position += 1


# job is either being run or moving to HPQ so remove queue position and update queue position for all jobs behind it
def remove_job_from_pricing_queue(current_jobid, now, move_to_HPQ=False):

    current_job = get_job_from_pricing_queue_dict(current_jobid)

    global job_pricing_queue_dict
    global completed_jobs_pricing_queue_dict
    global high_priority_queue_dict

    # if the job is moving from LPQ to HPQ then add it to the high_priority_queue__dict
    if move_to_HPQ is True:
        current_job.in_high_priority_queue = True
        high_priority_queue_dict[current_jobid] = current_job
        # print('Job Pricing Queue - Moving Job form LPQ to HPQ - ' + str(current_jobid))

    # otherwise move it to the completed jobs_pricing_queue_dict since the job is actually running
    else:
        completed_jobs_pricing_queue_dict[current_jobid] = current_job

        # print('Job Pricing Queue - Running LPQ job - ' + str(current_jobid))

        # compute estimated_slowdown_at_runtime
        current_walltime = float(current_job.walltime) * 60.0
        current_job.estimated_slowdown_at_runtime = ((now-current_job.submittime) + current_walltime) / current_walltime

    # remove job from previous dict and update pricing queue positions
    # if current_job.jobid in job_pricing_queue_dict:

    if current_jobid not in job_pricing_queue_dict:
        print('Error - current_jobid not in job_pricing_queue_dict - ' + str(current_jobid))
        print(job_pricing_queue_dict)
        exit(-1)
    # assert current_jobid in job_pricing_queue_dict

    del job_pricing_queue_dict[current_jobid]

    for job_id, temp_job in job_pricing_queue_dict.iteritems():
        if temp_job.pricing_queue_position > current_job.pricing_queue_position:
            temp_job.pricing_queue_position -= 1

    # print('Job Pricing Queue - removing job from job_pricing_queue_dict: ' + str(current_jobid))


# job is in HPQ and is about to be run so remove from high_priority_queue_dict

# we are about to run a RTJ - however RTJ could be in either high_priority_queue_dict or job_pricing_queue_dict
# so we much check both and then remove the job from the correct queue
def remove_realtime_job_from_pricing_queue(current_jobid, now):

    global completed_jobs_pricing_queue_dict
    global high_priority_queue_dict
    global job_pricing_queue_dict

    if current_jobid in high_priority_queue_dict:
        current_job = get_job_from_high_priority_queue_dict(current_jobid)

        if current_jobid not in high_priority_queue_dict:
            print('Error - current_jobid in high_priority_queue_dict - ' + str(current_jobid))
            print([(key, value.jobid) for key, value in high_priority_queue_dict.iteritems()])

            # print(high_priority_queue_dict)
            exit(-1)
        # assert current_jobid in high_priority_queue_dict

        del high_priority_queue_dict[current_jobid]
        # print('Job Pricing Queue - removing job from high_priority_queue_dict: ' + str(current_jobid))


    elif current_jobid in job_pricing_queue_dict:
        current_job = get_job_from_pricing_queue_dict(current_jobid)

        if current_jobid not in job_pricing_queue_dict:
            print('Error - current_jobid in job_pricing_queue_dict - ' + str(current_jobid))
            print([(key, value.jobid) for key, value in job_pricing_queue_dict])
            # print(job_pricing_queue_dict)
            exit(-1)
        # assert current_jobid in job_pricing_queue_dict

        del job_pricing_queue_dict[current_jobid]
        # print('Job Pricing Queue - removing job from job_pricing_queue_dict: ' + str(current_jobid))


    else:
        print('Error - in remove_realtime_job_from_pricing_queue - job not in either queue_dict: ' + str(current_jobid))
        exit(-1)

    completed_jobs_pricing_queue_dict[current_jobid] = current_job

    # compute estimated_slowdown_at_runtime
    current_walltime = float(current_job.walltime) * 60.0
    current_job.estimated_slowdown_at_runtime = ((now - current_job.submittime) + current_walltime) / current_walltime

# this sorts the jobs in the list based on the rtj pricing queue (RTJs have an assigned queue position, and the
# batch jobs fill in the spots in the queue based on their sorting using the utility function)
# assumes jobs are already sorted using utility function so batch jobs are sorted as well (they're a subset)
# this function is called by the simulator in compute_pricing_slowdown_quotes()
def sort_jobs_based_on_pricing_queue(all_jobs):

    rtj_pricing_positions = [tmp.pricing_queue_position for tmp in all_jobs if tmp.user == 'realtime']

    if len(rtj_pricing_positions) > 0:
        pass

    all_jobs.sort(utilitycmp)

    current_position = min_queue_position
    for temp_job in all_jobs:
        while current_position in rtj_pricing_positions:
            current_position += 1

        if temp_job.user != 'realtime':
            temp_job.pricing_queue_position = current_position
            current_position += 1

    all_jobs.sort(key=lambda x: x.pricing_queue_position)

    return all_jobs


# this sorts the jobs in the list based on the rtj pricing queue (RTJs have an assigned queue position, and the
# batch jobs fill in the spots in the queue based on their sorting using the utility function)
# assumes jobs are already sorted using utility function so batch jobs are sorted as well (they're a subset)
# this function is called by the scheduler in bgsched.py
def sort_scheduler_jobs_based_on_pricing_queue(all_jobs):

    rtj_pricing_positions = [tmp.pricing_queue_position for jobid, tmp in job_pricing_queue_dict.iteritems() if tmp.user == 'realtime']

    if len(rtj_pricing_positions) > 0:
        pass

    current_position = min_queue_position

    queue_position_tuples = []
    for temp_job in all_jobs:
        while current_position in rtj_pricing_positions:
            current_position += 1

        if temp_job.user != 'realtime':
            job_pricing_queue_dict[temp_job.jobid].pricing_queue_position = current_position
            current_tuple = (current_position, temp_job)
            current_position += 1
        else:
            current_tuple = (job_pricing_queue_dict[temp_job.jobid], temp_job)

        queue_position_tuples.append(current_tuple)

    queue_position_tuples.sort()

    new_jobs_list = [job for queue_position, job in queue_position_tuples]

    return new_jobs_list


# order the jobs with biggest utility first
def utilitycmp(job1, job2):
    return -cmp(job1.score, job2.score)


