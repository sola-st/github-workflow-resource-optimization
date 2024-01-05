import time
import datetime
from runs_analysis.name_based_analysis import simplify_and_map
from runs_analysis.resource_usage import calculate_costs
"""def get_wasted_schedule_1(all_runs, all_jobs, thresh):
    scheduled_runs_copy = all_runs[all_runs.event == "schedule"].copy()
    repos_list = scheduled_runs_copy.repo_id.unique().tolist()
    runs_dict = {}
    opt_runs=  []
    for repo in repos_list:
        repo_runs = scheduled_runs_copy[scheduled_runs_copy.repo_id==repo].sort_values("id")
        workflows = repo_runs.workflow_id.unique().tolist()
        repo_dict = {}
        for workflow in workflows:
            repo_dict[workflow] = []
            sublist = []
            for i, row in repo_runs[repo_runs.workflow_id==workflow].iterrows():
                if row["conclusion"] == "failure":
                    sublist.append((row["id"], row["repo_id"], row["workflow_id"]))
                else:
                    if len(sublist)>thresh:
                        #print(row["repo_id"], len(sublist))
                        opt_runs.append(sublist)
                    repo_dict[workflow].append(sublist)
                    sublist = []
        runs_dict[repo] = repo_dict
        
    total_waste_time = 0
    for run in opt_runs:
        total_waste_time += all_jobs[all_jobs.run_id.isin([r[0] for r in run[thresh:]])].up_time.sum()
        
    total_schedule_time = all_jobs[all_jobs.run_id.isin(scheduled_runs_copy.id)].up_time.sum()
    total_time = all_jobs.up_time.sum()
    impacted_runs = 0
    for run in opt_runs:
        impacted_runs += len(run[thresh:])

    print(total_time)
    print(total_schedule_time/total_time)
    return total_waste_time, round(total_waste_time/total_schedule_time,4), round(total_waste_time/total_time, 4), round(impacted_runs/all_runs.shape[0], 4)
"""

def get_wasted_schedule_1(all_runs, all_jobs, thresh):
    scheduled_runs_copy = all_runs[all_runs.event == "schedule"].copy()
    repos_list = scheduled_runs_copy.repo_id.unique().tolist()
    runs_dict = {}
    opt_runs=  []
    opt_repos = []
    for repo in repos_list:
        repo_runs = scheduled_runs_copy[scheduled_runs_copy.repo_id==repo].sort_values("id")
        workflows = repo_runs.workflow_id.unique().tolist()
        repo_dict = {}
        for workflow in workflows:
            repo_dict[workflow] = []
            sublist = []
            for i, row in repo_runs[repo_runs.workflow_id==workflow].iterrows():
                if row["conclusion"] == "failure":
                    sublist.append((row["id"], row["repo_id"], row["workflow_id"]))
                else:
                    if len(sublist)>thresh:
                        #print(row["repo_id"], len(sublist))
                        opt_runs.append(sublist)
                        opt_repos.append(repo)
                    repo_dict[workflow].append(sublist)
                    sublist = []
        runs_dict[repo] = repo_dict
        
    total_waste_time = 0
    for run in opt_runs:
        total_waste_time += all_jobs[all_jobs.run_id.isin([r[0] for r in run[thresh:]])].up_time.sum()
        
    total_schedule_time = all_jobs[all_jobs.run_id.isin(scheduled_runs_copy.id)].up_time.sum()
    total_time = all_jobs.up_time.sum()
    impacted_runs = 0
    for run in opt_runs:
        impacted_runs += len(run[thresh:])

    #print(total_time)
    #print(total_schedule_time/total_time)
    return total_waste_time, \
        round(total_waste_time/total_schedule_time,4), \
        round(total_waste_time/total_time, 4),  \
        round(impacted_runs/all_runs[all_runs.event=="schedule"].shape[0], 4), \
        round(impacted_runs/all_runs.shape[0], 4), opt_repos 
 
def is_commit_inbetween(ts, previous_ts, repo_name, commits):
    if repo_name in commits:
        for cm_ts in commits[repo_name]:
            if cm_ts < ts and previous_ts < cm_ts:
                return True
        return False
    else:
        return False

"""def get_wasted_schedule_2(all_runs, all_jobs, all_repos, commits):
    scheduled_runs_copy = all_runs[all_runs.event == "schedule"].copy()
    repos_list = scheduled_runs_copy.repo_id.unique().tolist()
    wasted_fails = []
    previous_run_ts = -1
    for repo in repos_list:
        repo_runs = scheduled_runs_copy[scheduled_runs_copy.repo_id==repo].sort_values("id")
        repo_name = all_repos[all_repos.id==repo]["full_name"].to_list()[0]
        workflows = repo_runs.workflow_id.unique().tolist()
        for workflow in workflows:
            is_previous_failed = False
            for i, row in repo_runs[repo_runs.workflow_id==workflow].iterrows():
                if row["conclusion"] == "failure":
                    run_ts = int(time.mktime(datetime.datetime.strptime(row["created_at"], "%Y-%m-%dT%H:%M:%SZ").timetuple()))
                    if is_previous_failed:
                        is_interrupted = is_commit_inbetween(run_ts, previous_run_ts,repo_name, commits)
                        if not is_interrupted:
                            wasted_fails.append(row["id"])
                    is_previous_failed = True
                    previous_runs_ts = run_ts
                else:
                    is_previous_failed = False
    total_waste_time = all_jobs[all_jobs.run_id.isin(wasted_fails)].up_time.sum()
    total_time = all_jobs.up_time.sum()
    total_schedule_time = all_jobs[all_jobs.run_id.isin(scheduled_runs_copy.id)].up_time.sum()
    print(total_time)
    print(total_schedule_time)
    return total_waste_time, round(total_waste_time/total_schedule_time,4), round(total_waste_time/total_time, 4)"""

def get_wasted_schedule_2(all_runs, all_jobs, all_repos, commits):
    scheduled_runs_copy = all_runs[all_runs.event == "schedule"].copy()
    repos_list = scheduled_runs_copy.repo_id.unique().tolist()
    wasted_fails = []
    previous_run_ts = -1
    for repo in repos_list:
        repo_runs = scheduled_runs_copy[scheduled_runs_copy.repo_id==repo].sort_values("id")
        repo_name = all_repos[all_repos.id==repo]["full_name"].to_list()[0]
        workflows = repo_runs.workflow_id.unique().tolist()
        for workflow in workflows:
            is_previous_failed = False
            for i, row in repo_runs[repo_runs.workflow_id==workflow].iterrows():
                if row["conclusion"] == "failure":
                    run_ts = int(time.mktime(datetime.datetime.strptime(row["created_at"], "%Y-%m-%dT%H:%M:%SZ").timetuple()))
                    if is_previous_failed:
                        is_interrupted = is_commit_inbetween(run_ts, previous_run_ts,repo_name, commits)
                        if not is_interrupted:
                            wasted_fails.append(row["id"])
                    is_previous_failed = True
                    previous_run_ts = run_ts
                else:
                    is_previous_failed = False
    total_waste_time = all_jobs[all_jobs.run_id.isin(wasted_fails)].up_time.sum()
    total_time = all_jobs.up_time.sum()
    total_schedule_time = all_jobs[all_jobs.run_id.isin(scheduled_runs_copy.id)].up_time.sum()
    print(total_time)
    print(total_schedule_time)
    return total_waste_time, round(total_waste_time/total_schedule_time,4), round(total_waste_time/total_time, 4), wasted_fails

def test_build_heuristic(data_set, commits_dict):
    all_jobs = data_set.get_all_jobs()
    all_runs = data_set.get_all_runs()
    all_runs["start_ts"] = all_runs.created_at.apply(lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    scheduled_runs = all_runs[all_runs.event=="schedule"]
    simplified_jobs = simplify_and_map(data_set)
    scheduled_test_build = scheduled_runs[scheduled_runs.id.isin(simplified_jobs[simplified_jobs.sub_names.isin(["build", "test"])].run_id)]
    all_repos = data_set.get_all_repositories()
    repos_names = all_repos[all_repos.id.isin(scheduled_test_build.repo_id.unique())].full_name.to_list()
    scheduled_test_build_repo = scheduled_test_build.merge(all_repos[["id", "full_name"]], left_on="repo_id", right_on="id")
    wasted_runs_ids = []
    for repo in repos_names:
        if repo in commits_dict:
            repo_runs = scheduled_test_build_repo[scheduled_test_build_repo.full_name==repo]
            previous = False
            for i, row in repo_runs.iterrows():
                run_ts = row["start_ts"]
                if previous:
                    if not is_commit_inbetween(run_ts, previous_ts, repo, commits_dict):
                         wasted_runs_ids.append(row["id_x"])
                previous_ts = run_ts
                previous = True
    wasted_time = all_jobs[all_jobs.run_id.isin(wasted_runs_ids)].up_time.sum()
    total_time = all_jobs[all_jobs.run_id.isin(scheduled_test_build_repo[scheduled_test_build_repo.full_name.isin(commits_dict.keys())].id_x)].up_time.sum()
    all_runs_repos = all_runs.merge(all_repos[["id", "full_name"]], left_on="repo_id", right_on="id")
    total_repos_time = all_jobs[all_jobs.run_id.isin(all_runs_repos[all_runs_repos.full_name.isin(commits_dict.keys())].id_x)].up_time.sum()
    
    return wasted_time, total_time, total_repos_time

"""def failed_jobs_prioritization(data_set):
    all_runs = data_set.get_all_runs()
    all_runs["start_ts"] = all_runs.created_at.apply(lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    all_jobs = data_set.get_all_jobs()
    repos_ids = all_runs.repo_id.unique()
    to_save = []
    for repo_id in repos_ids:
        repo_runs = all_runs[all_runs.repo_id==repo_id]
        repo_runs = repo_runs[repo_runs.total_count > 1]
        workflows_ids = repo_runs.workflow_id.unique()
        for workflow_id in workflows_ids:
            for i, row in repo_runs[(repo_runs.workflow_id==workflow_id)].sort_values("run_number").iterrows():
                if row["conclusion"] == "failure":
                    workflow_jobs = all_jobs[all_jobs.run_id==row["id"]].sort_values("start_ts")
                    if workflow_jobs[workflow_jobs.conclusion=="failure"].shape[0] == -1: ## > 1
                        continue
                    elif  workflow_jobs[(workflow_jobs.conclusion=="failure") | (workflow_jobs.conclusion=="cancelled")].shape[0] == 0:
                        #print(row["id"])
                        continue
                    else:
                        fail_ts = workflow_jobs[(workflow_jobs.conclusion=="failure")|(workflow_jobs.conclusion=="cancelled")].end_ts.to_list()[0]
                        if workflow_jobs[workflow_jobs.end_ts>fail_ts].shape[0]==-1:## !=0
                            continue
                        else:
                            rank = 0
                            jobs_to_save = []
                            for j, job in workflow_jobs.iterrows():
                                if job["conclusion"]=="success":
                                    jobs_to_save.append(job["id"])
                                    rank += 1
                                else:
                                    failed_job_id = job["id"]
                                    failed_job_name = job["name"]
                                    failed_job_rank = rank
                    if previous_failure:
                        if (previous_failed_rank == failed_job_rank) and (previous_failed_name== failed_job_name):
                            to_save.append(previous_jobs_to_save)
                    previous_failure= True
                    previous_failed_rank = failed_job_rank
                    previous_failed_name = failed_job_name
                    previous_jobs_to_save = jobs_to_save
                else:
                    previous_failure = False
    inlined_ids = []
    for id_list in to_save:
        inlined_ids.extend(id_list)
    time_saved_overall = all_jobs[all_jobs.id.isin(inlined_ids)].up_time.sum()/all_jobs.up_time.sum()*100
    time_saved_over_impacted = all_jobs[all_jobs.id.isin(inlined_ids)].up_time.sum()/all_jobs[all_jobs.run_id.isin(all_jobs[all_jobs.id.isin(inlined_ids)].run_id)].up_time.sum() * 100
    impacted_runs = all_jobs[all_jobs.id.isin(inlined_ids)].run_id.unique().shape[0]/all_runs.shape[0]*100
    
    return time_saved_overall, time_saved_over_impacted, impacted_runs
"""


def failed_jobs_prioritization(data_set, repos_list):
    all_runs = data_set.get_all_runs()
    all_runs = all_runs[all_runs.repo_id.isin(repos_list)]
    all_runs["start_ts"] = all_runs.created_at.apply(lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    all_jobs = data_set.get_all_jobs()
    repos_ids = all_runs.repo_id.unique()
    to_save = []
    for repo_id in repos_ids:
        repo_runs = all_runs[all_runs.repo_id==repo_id]
        repo_runs = repo_runs[repo_runs.total_count > 1]
        workflows_ids = repo_runs.workflow_id.unique()
        for workflow_id in workflows_ids:
            for i, row in repo_runs[(repo_runs.workflow_id==workflow_id)].sort_values("run_number").iterrows():
                if row["conclusion"] == "failure":
                    workflow_jobs = all_jobs[all_jobs.run_id==row["id"]].sort_values("start_ts")
                    if workflow_jobs[workflow_jobs.conclusion=="failure"].shape[0] == -1: ## > 1
                        continue
                    elif  workflow_jobs[(workflow_jobs.conclusion=="failure") | (workflow_jobs.conclusion=="cancelled")].shape[0] == 0:
                        #print(row["id"])
                        continue
                    else:
                        fail_ts = workflow_jobs[(workflow_jobs.conclusion=="failure")|(workflow_jobs.conclusion=="cancelled")].end_ts.to_list()[0]
                        if workflow_jobs[workflow_jobs.end_ts>fail_ts].shape[0]==-1:## !=0
                            continue
                        else:
                            rank = 0
                            jobs_to_save = []
                            for j, job in workflow_jobs.iterrows():
                                if job["conclusion"]=="success":
                                    jobs_to_save.append(job["id"])
                                    rank += 1
                                else:
                                    failed_job_id = job["id"]
                                    failed_job_name = job["name"]
                                    failed_job_rank = rank
                    if previous_failure:
                        if (previous_failed_rank == failed_job_rank) and (previous_failed_name== failed_job_name):
                            to_save.append(previous_jobs_to_save)
                    previous_failure= True
                    previous_failed_rank = failed_job_rank
                    previous_failed_name = failed_job_name
                    previous_jobs_to_save = jobs_to_save
                else:
                    previous_failure = False
    inlined_ids = []
    for id_list in to_save:
        inlined_ids.extend(id_list)
    time_saved_overall = all_jobs[all_jobs.id.isin(inlined_ids)].up_time.sum()/all_jobs.up_time.sum()*100
    time_saved_over_impacted = all_jobs[all_jobs.id.isin(inlined_ids)].up_time.sum()/all_jobs[all_jobs.run_id.isin(all_jobs[all_jobs.id.isin(inlined_ids)].run_id)].up_time.sum() * 100
    impacted_runs = all_jobs[all_jobs.id.isin(inlined_ids)].run_id.unique().shape[0]/all_runs.shape[0]*100
    
    return time_saved_overall, time_saved_over_impacted, impacted_runs, inlined_ids

"""def timeout_value_optimization(data_set):
    all_runs = data_set.get_all_runs()
    all_jobs = data_set.get_all_jobs()
    runs_total_time = all_jobs.groupby("run_id").agg({"up_time": "sum"}).reset_index()
    all_runs_time = all_runs.merge(runs_total_time, left_on="id", right_on="run_id")
    all_runs_time["start_ts"] = all_runs_time.created_at.apply(lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    saved_time = []
    total_impact_time = []
    impacted_runs = []
    window = 10
    fraction = 0.1
    workflows_ids = all_runs.workflow_id.unique().tolist()
    for workflow_id in workflows_ids:
        workflow_runs = all_runs_time[all_runs_time.workflow_id==workflow_id].sort_values("run_number")
        if workflow_runs.shape[0]==0:
            continue
        else:
            workflow_jobs = all_jobs[all_jobs.run_id.isin(workflow_runs.id)]
            jobs_names = workflow_jobs.name.unique().tolist()
            for job_name in jobs_names:
                jobs = workflow_jobs[workflow_jobs.name==job_name]
                max_time = jobs[jobs.up_time<=21541].up_time.max()
                target_jobs = jobs[jobs.up_time>21541]
                if target_jobs.shape[0]!=0:
                    saved_time.append(target_jobs.up_time.sum() - target_jobs.shape[0] * max_time*1.1)
                    impacted_runs.extend(target_jobs.run_id.to_list())
                    
    return saved_time, impacted_runs"""

def timeout_value_optimization(data_set, repos_list):
    all_runs = data_set.get_all_runs()
    all_runs = all_runs[all_runs.repo_id.isin(repos_list)]
    all_jobs = data_set.get_all_jobs()
    runs_total_time = all_jobs.groupby("run_id").agg({"up_time": "sum"}).reset_index()
    all_runs_time = all_runs.merge(runs_total_time, left_on="id", right_on="run_id")
    all_runs_time["start_ts"] = all_runs_time.created_at.apply(lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    saved_time = []
    total_impact_time = []
    impacted_runs = []
    window = 10
    fraction = 0.1
    workflows_ids = all_runs.workflow_id.unique().tolist()
    for workflow_id in workflows_ids:
        workflow_runs = all_runs_time[all_runs_time.workflow_id==workflow_id].sort_values("run_number")
        if workflow_runs.shape[0]==0:
            continue
        else:
            workflow_jobs = all_jobs[all_jobs.run_id.isin(workflow_runs.id)]
            jobs_names = workflow_jobs.name.unique().tolist()
            for job_name in jobs_names:
                jobs = workflow_jobs[workflow_jobs.name==job_name]
                max_time = jobs[jobs.up_time<=21541].up_time.max()
                target_jobs = jobs[jobs.up_time>21541]
                if target_jobs.shape[0]!=0:
                    saved_time.append(target_jobs.up_time.sum() - target_jobs.shape[0] * max_time*1.1)
                    impacted_runs.extend(target_jobs.run_id.to_list())
                    
    return saved_time, impacted_runs


def deactivate_inactive_schedule(data_set, commits_dict):
    all_runs = data_set.get_all_runs()
    all_jobs = data_set.get_all_jobs()
    runs_total_time = all_jobs.groupby("run_id").agg({"up_time": "sum"}).reset_index()
    all_runs_time = all_runs.merge(runs_total_time, left_on="id", right_on="run_id")
    all_repos = data_set.get_all_repositories()
    all_runs_time["start_ts"] = all_runs_time.created_at.apply(lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    runs_to_save = []
    for repo in all_runs_time.repo_id.unique():
        repo_name = all_repos[all_repos.id==repo].full_name.to_list()[0]
        print(repo_name)
        if repo_name in commits_dict:
            scheduled_repo_runs = all_runs_time[(all_runs_time.event=="schedule") & (all_runs_time.repo_id==repo)]
            workflows_ids = scheduled_repo_runs.workflow_id.unique().tolist()
            print("\t", len(workflows_ids))
            for workflow_id in workflows_ids:
                to_optimize = []
                workflow_runs = scheduled_repo_runs[scheduled_repo_runs.workflow_id==workflow_id]
                timestamps = workflow_runs.start_ts.to_list()
                timestamps_label = [(ts, "run") for ts in timestamps]
                commits_ts = [(ts, "commit") for ts in commits_dict[repo_name]]
                runs_commits_ts = timestamps_label + commits_ts
                runs_commits_ts.sort()
                start = None
                
                for i in range(len(runs_commits_ts)):
                    if runs_commits_ts[i][1] == "commit":
                        continue
                    else:
                        start = i
                        break
                previous = True
                for rct in runs_commits_ts[1:]:
                    if rct[1] == "run" and previous:
                        to_optimize.append(rct[0])
                    elif rct[1] == "run":
                        previous = True
                    else:
                        previous = False
                runs_to_save.extend(workflow_runs[workflow_runs.start_ts.isin(to_optimize)].id.to_list())

    return runs_to_save

def compute_wasted_schedule1(all_runs, all_jobs, repos_list):
    all_runs_sub = all_runs[all_runs.repo_id.isin(repos_list)]
    total_waste, waste_over_total_schedule, total_over_total, impacted_runs_schedule, impacted_runs_all, opt_repos = get_wasted_schedule_1(all_runs_sub, all_jobs, 3)
    all_runs["start_ts"] = all_runs.created_at.apply(lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    sub_runs = all_runs[all_runs.repo_id.isin(opt_repos)]
    min_max_start_ts = sub_runs.groupby("repo_id").start_ts.agg(["min", "max"]).reset_index()
    total_start_ts = 0
    for i, row in min_max_start_ts.iterrows():
        total_start_ts += row["max"] - row["min"]
    years = total_start_ts/(12*30*24*3600)
    saved_cost = calculate_costs(total_waste / 60 / years)
    return impacted_runs_all, impacted_runs_schedule, total_over_total, waste_over_total_schedule, saved_cost


def compute_wasted_schedule2(all_runs, all_jobs, all_repos, commits_dict, repos_list):
    all_runs_sub = all_runs[all_runs.repo_id.isin(repos_list)]
    total_waste_time, total_over_schedule, total_over_total, wasted_fails = get_wasted_schedule_2(all_runs_sub, all_jobs, all_repos, commits_dict)
    sub_runs = all_runs[all_runs.id.isin(wasted_fails)]
    min_max_start_ts = sub_runs.groupby("repo_id").start_ts.agg(["min", "max"]).reset_index()
    total_start_ts = 0
    for i, row in min_max_start_ts.iterrows():
        total_start_ts += row["max"] - row["min"]
    years = total_start_ts/(12*30*24*3600)
    saved_cost = calculate_costs(total_waste_time / 60 / years)
    return all_runs_sub, total_waste_time, total_over_schedule, total_over_total, wasted_fails, saved_cost