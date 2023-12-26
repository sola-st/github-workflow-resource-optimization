import time
import datetime

def get_resource_usage_summary(data_set):
    all_runs = data_set.get_all_runs().drop_duplicates(subset=["id", "run_attempt"])
    all_jobs = data_set.get_all_jobs().drop_duplicates()
    all_steps = data_set.get_all_steps()

    all_runs_ids = all_runs[["id", "repo_id"]]
    all_jobs_with_repos = all_jobs.merge(all_runs_ids, left_on="run_id", right_on="id")
    median_time_per_repo = all_jobs_with_repos.groupby("repo_id").agg({"up_time": "sum"}).reset_index()["up_time"].median()
    avg_time_per_repo = all_jobs_with_repos.groupby("repo_id").agg({"up_time": "sum"}).reset_index()["up_time"].mean()
    time_by_repo = all_jobs_with_repos[all_jobs_with_repos.completed_at.notnull()].groupby("repo_id").agg({"up_time": "sum", "started_at": "min", "completed_at": "max"}).reset_index()
    

    time_by_repo["min_ts"] = time_by_repo["started_at"].apply(
        lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    time_by_repo["max_ts"] = time_by_repo["completed_at"].apply(
        lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    
    time_by_repo["monthly_time"] = time_by_repo["up_time"]/((time_by_repo["max_ts"]-time_by_repo["min_ts"])/(24*3600*30))
    
    return {
        "runs number": all_runs.shape[0],
        "repos number": all_runs.repo_id.unique().shape[0],
        "jobs number": all_jobs.shape[0],
        "steps number": all_steps.shape[0],
        "total time": all_jobs.up_time.sum()/60,
        "repo median time": median_time_per_repo/60,
        "repo average": avg_time_per_repo/60,
        "monthly median time": time_by_repo.monthly_time.median()/60,
        "monthly average time": time_by_repo.monthly_time.mean()/60,
        "events": all_runs.event.unique().tolist()
    }

## outdated
"""def triggering_events_proportion(data_set):
    all_runs = data_set.get_all_runs()
    trigger_events = all_runs.groupby("event").agg({"id": "count"}).reset_index()
    trigger_events["proportion"] = trigger_events.id / all_runs.shape[0] * 100
    return trigger_events.sort_values("proportion", ascending=False)[["event", "proportion"]].set_index("event").to_dict()["proportion"]
"""

def triggering_events_proportion(data_set, repos_list):
    all_runs = data_set.get_all_runs()
    all_runs = all_runs[all_runs.repo_id.isin(repos_list)]
    trigger_events = all_runs.groupby("event").agg({"id": "count"}).reset_index()
    trigger_events["proportion"] = trigger_events.id / all_runs.shape[0] * 100
    return trigger_events.sort_values("proportion", ascending=False)[["event", "proportion"]].set_index("event").to_dict()["proportion"]

"""
def triggering_events_time_proportion(data_set):
    all_runs = data_set.get_all_runs()
    all_jobs = data_set.get_all_jobs()
    runs_jobs_table = all_runs.merge(all_jobs, left_on="id", right_on="run_id")[["run_id", "event", "up_time"]]
    up_time_agg = runs_jobs_table.groupby("event").agg({"up_time": "sum"}).reset_index()
    up_time_agg["up_time_prop"] = up_time_agg["up_time"]*100 / up_time_agg.up_time.sum()
    return up_time_agg.sort_values("up_time_prop")[["event", "up_time_prop"]].set_index("event").to_dict()["up_time_prop"]

"""

def triggering_events_time_proportion(data_set, repos_list):
    all_runs = data_set.get_all_runs()
    all_runs = all_runs[all_runs.repo_id.isin(repos_list)]
    all_jobs = data_set.get_all_jobs()
    runs_jobs_table = all_runs.merge(all_jobs, left_on="id", right_on="run_id")[["run_id", "event", "up_time"]]
    up_time_agg = runs_jobs_table.groupby("event").agg({"up_time": "sum"}).reset_index()
    up_time_agg["up_time_prop"] = up_time_agg["up_time"]*100 / up_time_agg.up_time.sum()
    return up_time_agg.sort_values("up_time_prop")[["event", "up_time_prop"]].set_index("event").to_dict()["up_time_prop"]

"""def get_avg_runtime_by_event(data_set):
    all_runs = data_set.get_all_runs()
    all_jobs = data_set.get_all_jobs()

    events_list = all_runs.event.unique().tolist()
    
    avg_per_event = {}
    for event in events_list:
        event_df = all_runs[all_runs.event==event]
        jobs_df = all_jobs[all_jobs.run_id.isin(event_df.id)]
        jobs_df_agg = jobs_df.groupby("run_id").agg({"up_time": "sum"}).reset_index()
        avg_per_event[event] = (jobs_df_agg.up_time.mean()/60, jobs_df_agg.up_time.median()/60)

    return avg_per_event"""

def get_avg_runtime_by_event(data_set, repos_list):
    all_runs = data_set.get_all_runs()
    all_runs = all_runs[all_runs.repo_id.isin(repos_list)]
    all_jobs = data_set.get_all_jobs()

    events_list = all_runs.event.unique().tolist()
    
    avg_per_event = {}
    for event in events_list:
        event_df = all_runs[all_runs.event==event]
        jobs_df = all_jobs[all_jobs.run_id.isin(event_df.id)]
        jobs_df_agg = jobs_df.groupby("run_id").agg({"up_time": "sum"}).reset_index()
        jobs_df_agg = jobs_df_agg[jobs_df_agg.up_time > 0]
        avg_per_event[event] = (jobs_df_agg.up_time.mean()/60, 
                                jobs_df_agg.up_time.median()/60, 
                                (jobs_df_agg.up_time - jobs_df_agg.up_time.mean()).abs().mean()/60, 
                                (jobs_df_agg.up_time.quantile(0.75) - jobs_df_agg.up_time.quantile(0.25))/60)

    return avg_per_event

def get_monthly_time_by_repo(data_set):
    all_runs = data_set.get_all_runs().drop_duplicates(subset=["id", "run_attempt"])
    all_jobs = data_set.get_all_jobs().drop_duplicates()

    all_runs_ids = all_runs[["id", "repo_id"]]
    all_jobs_with_repos = all_jobs.merge(all_runs_ids, left_on="run_id", right_on="id")
    time_by_repo = all_jobs_with_repos[all_jobs_with_repos.completed_at.notnull()].groupby("repo_id").agg({"up_time": "sum", "started_at": "min", "completed_at": "max"}).reset_index()

    time_by_repo["min_ts"] = time_by_repo["started_at"].apply(
        lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    time_by_repo["max_ts"] = time_by_repo["completed_at"].apply(
        lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    
    time_by_repo["monthly_time"] = time_by_repo["up_time"]/((time_by_repo["max_ts"]-time_by_repo["min_ts"])/(24*3600*30))
    
    return time_by_repo

def get_os_factor():
    # This value calculated as an average of os usage in our dataset,
    # To be able to include unknown os, we replace all data points with average
    # TODO: future versions should replace the hardcoded value with the process of calculating this value
    return 1.52

def get_minute_cost():
    # This is based on the costs of GitHub Actions, it might change over time
    # Get new values from here: https://docs.github.com/en/billing/managing-billing-for-github-actions/about-billing-for-github-actions
    return 0.008

def get_tiers(data_set):
    all_jobs = data_set.get_all_jobs()
    all_runs = data_set.get_all_runs()
    jobs_runs_time = all_jobs.groupby("run_id").agg({"up_time": "sum", "start_ts": "min"}).reset_index()
    runs_with_time = all_runs.merge(jobs_runs_time, left_on="id", right_on="run_id")
    repos_list_1 = []
    month_time = 30 * 24 * 3600
    os_factor = get_os_factor()
    thresh = 2000*60
    for repo_id in runs_with_time.repo_id.unique():
        min_start_ts = runs_with_time[(runs_with_time.repo_id==repo_id) & (runs_with_time.start_ts_y!=-1)]["start_ts_y"].min()
        current_ts = min_start_ts
        max_start_ts = runs_with_time[runs_with_time.repo_id==repo_id]["start_ts_y"].max()
        while current_ts <= max_start_ts:
            vm_time = runs_with_time[(runs_with_time.repo_id == repo_id) & 
                                     (runs_with_time.start_ts_y<current_ts+month_time) & 
                                     (runs_with_time.start_ts_y>=current_ts)].up_time.sum()
            if os_factor * vm_time > thresh:
                repos_list_1.append(repo_id)
                break
            else:
                current_ts += month_time
    repos_list_2 = [repo_id for repo_id in all_runs.repo_id.unique() if repo_id not in repos_list_1]
    
    return repos_list_1, repos_list_2

def get_comparative_resource_usage_summary(data_set, list_1, list_2):
    all_runs = data_set.get_all_runs().drop_duplicates(subset=["id", "run_attempt"])
    runs_list_1 = all_runs[all_runs.repo_id.isin(list_1)]
    runs_list_2 = all_runs[all_runs.repo_id.isin(list_2)]
    
    all_jobs = data_set.get_all_jobs().drop_duplicates()
    jobs_list_1 = all_jobs[all_jobs.run_id.isin(runs_list_1.id.unique())]
    jobs_list_2 = all_jobs[all_jobs.run_id.isin(runs_list_2.id.unique())]
    
    all_steps = data_set.get_all_steps()
    steps_list_1 = all_steps[all_steps.job_id.isin(jobs_list_1.id.unique())]
    steps_list_2 = all_steps[all_steps.job_id.isin(jobs_list_2.id.unique())]
    
    runs_list_1_ids = runs_list_1[["id", "repo_id"]]
    runs_list_2_ids = runs_list_2[["id", "repo_id"]]
    
    jobs_list_1_with_repos = all_jobs.merge(runs_list_1_ids, left_on="run_id", right_on="id")
    jobs_list_2_with_repos = all_jobs.merge(runs_list_2_ids, left_on="run_id", right_on="id")
    
    up_time_sum_list_1 = jobs_list_1_with_repos.groupby("repo_id").agg({"up_time": "sum"}).reset_index()
    up_time_sum_list_2 = jobs_list_2_with_repos.groupby("repo_id").agg({"up_time": "sum"}).reset_index()
    
    median_list_1 = up_time_sum_list_1.up_time.median()
    mean_list_1 = up_time_sum_list_1.up_time.mean()
    std_list_1 = up_time_sum_list_1.up_time.std()
    
    
    median_list_2 = up_time_sum_list_2.up_time.median()
    mean_list_2 = up_time_sum_list_2.up_time.mean()
    std_list_2 = up_time_sum_list_2.up_time.std()
    
    
    time_by_repo_1 = jobs_list_1_with_repos.groupby("repo_id").agg(up_time=("up_time", "sum"), min_ts=("start_ts", "min"), max_ts=("start_ts", "max")).reset_index()

    time_by_repo_1["live_time"] = time_by_repo_1["max_ts"] - time_by_repo_1["min_ts"]
    time_by_repo_1["live_time"] = time_by_repo_1["live_time"].apply(lambda x: 1 if x < 24*3600*30 else x / (30*24*3600))
    time_by_repo_1["monthly_time"] = time_by_repo_1["up_time"]*1.55/(time_by_repo_1["live_time"])
    
    
    time_by_repo_2= jobs_list_2_with_repos.groupby("repo_id").agg(up_time=("up_time", "sum"), min_ts=("start_ts", "min"), max_ts=("start_ts", "max")).reset_index()
    
    time_by_repo_2["live_time"] = time_by_repo_2["max_ts"] - time_by_repo_2["min_ts"]
    time_by_repo_2["live_time"] = time_by_repo_2["live_time"].apply(lambda x: 1 if x < 24*3600*30 else x / (30*24*3600))
    time_by_repo_2["monthly_time"] = time_by_repo_2["up_time"]*1.55/(time_by_repo_2["live_time"])

    #return time_by_repo_1
    
    return {
        "repos_list_1": {
            "runs number": runs_list_1.shape[0],
            "repos number": runs_list_1.repo_id.unique().shape[0],
            "jobs number": jobs_list_1.shape[0],
            "steps number": steps_list_1.shape[0],
            "total time": jobs_list_1.up_time.sum()/60,
            "repo median time": median_list_1/60,
            "repo mean time": mean_list_1/60,
            "repo std time": std_list_1,
            "iqr": (time_by_repo_1.monthly_time.quantile(.75) - time_by_repo_1.monthly_time.quantile(.25))/60,
            "monthly median time": time_by_repo_1.monthly_time.median()/60,
            "monthly mean time": time_by_repo_1.monthly_time.mean()/60,
            "monthly std time": time_by_repo_1.monthly_time.std()/60,
            "events": runs_list_1.event.unique().tolist()
        },
        
        "repos_list_2": {
            "runs number": runs_list_2.shape[0],
            "repos number": runs_list_2.repo_id.unique().shape[0],
            "jobs number": jobs_list_2.shape[0],
            "steps number": steps_list_2.shape[0],
            "total time": jobs_list_2.up_time.sum()/60,
            "repo median time": median_list_2/60,
            "repo mean time": mean_list_2/60,
            "repo std time": std_list_2,
            "iqr": (time_by_repo_2.monthly_time.quantile(.75) - time_by_repo_2.monthly_time.quantile(.25))/60,
            "monthly median time": time_by_repo_2.monthly_time.median()/60,
            "monthly mean time": time_by_repo_2.monthly_time.mean()/60,
            "monthly std time": time_by_repo_2.monthly_time.std()/60,
            "events": runs_list_2.event.unique().tolist()
        }
    }

def calc_costs_by_event(time, event):
    events_factor = {

    }
    # TODO(UPDATE BASED ON EVENT OS FACTOR)
    return round(time, 1) * get_os_factor() * get_minute_cost()

def calculate_costs(time):
    return round(time * get_os_factor() * get_minute_cost())

def get_avg_runtime_rest(data_set, repos_list, event_list):
    all_runs = data_set.get_all_runs()
    all_runs = all_runs[all_runs.repo_id.isin(repos_list)]
    all_jobs = data_set.get_all_jobs()

    events_list = all_runs.event.unique().tolist()
    
    #avg_per_event = {}
    #for event in events_list:
    event_df = all_runs[~all_runs.event.isin(event_list)]
    jobs_df = all_jobs[all_jobs.run_id.isin(event_df.id)]
    jobs_df_agg = jobs_df.groupby("run_id").agg({"up_time": "sum"}).reset_index()
    jobs_df_agg = jobs_df_agg[jobs_df_agg.up_time > 0]
    return (jobs_df_agg.up_time.mean()/60, 
                            jobs_df_agg.up_time.median()/60, 
                            (jobs_df_agg.up_time - jobs_df_agg.up_time.mean()).abs().mean()/60, 
                            (jobs_df_agg.up_time.quantile(0.75) - jobs_df_agg.up_time.quantile(0.25))/60)