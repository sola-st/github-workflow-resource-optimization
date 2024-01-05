import time
import datetime
import numpy as np

def get_optimization_usage(collected_commits, optimization="cache@"):
    optimization_added = []
    optimization_removed =  []
    created_with_optimization = []
    for item in collected_commits:
        for mdfile in item["Modified"]:
            #pprint(mdfile.export_as_dict())
            file_name = mdfile["file_name"]
            added_lines = [c[1] for c in mdfile["diff_parsed"]["added"]]
            removed_lines = [c[1] for c in mdfile["diff_parsed"]["deleted"]]
            added = False
            for al in added_lines:
                if optimization in al.lower().replace(" ", "").replace("\t", "") and not al.startswith("#"):
                    optimization_added.append((mdfile["repo"], file_name, int(mdfile["c_date"])))
                    added = True
                    break
            if not added:
                for rl in removed_lines:
                    if optimization in rl.lower().replace(" ", "").replace("\t", "") and not al.startswith("#"):
                        optimization_removed.append((mdfile["repo"], file_name, int(mdfile["c_date"])))
        for c_file in item["Added"]:
            file_name = c_file["file_name"]
            #pprint(c_file.export_as_dict())
            added_lines = [c[1] for c in c_file["diff_parsed"]["added"]]
            for al in added_lines:
                if optimization in al.lower().replace(" ", "").replace("\t", "") and not al.startswith("#"):
                    created_with_optimization.append((c_file["repo"], file_name, int(c_file["c_date"])))

    return {    
        "optimization_added": optimization_added, 
        "optimization_removed": optimization_removed, 
        "created_with_optimization": created_with_optimization
    }

def summarize_optimization_usage(collected_commits, optimization="cache@"):
    optimization_dict = get_optimization_usage(collected_commits, optimization=optimization)
    optimization_added = optimization_dict["optimization_added"]
    optimization_removed = optimization_dict["optimization_removed"]
    created_with_optimization = optimization_dict["created_with_optimization"]
    
    optimization_files_number = len(set(optimization_added)|set(created_with_optimization))
    optimization_repos_number = len(set([ca[0] for ca in optimization_added])|set([ca[0] for ca in created_with_optimization]))

    return {
        "optimization_files_number": optimization_files_number,
        "optimization_repos_number": optimization_repos_number
    }


def get_optimization_ts(collected_commits, optimization="cache@"):
    optimization_dict = get_optimization_usage(collected_commits, optimization=optimization)
    optimization_added = optimization_dict["optimization_added"]
    optimization_removed = optimization_dict["optimization_removed"]
    created_with_optimization = optimization_dict["created_with_optimization"]
    all_workflows = [(*tup, "added") for tup in optimization_added] + [(*tup, "removed") for tup in optimization_removed] + [(*tup, "added") for tup in created_with_optimization]
    ref_temp = 1679310197
    optimization_history = []
    workflows_set = list(set([(ca[0], ca[1]) for ca in optimization_added]+[(cwc[0], cwc[1]) for cwc in created_with_optimization]))
    for i in range(len(workflows_set)):
        repo_name = workflows_set[i][0]
        wf = workflows_set[i][1]
        specific_workflow = [tup for tup in all_workflows if tup[0]==repo_name and tup[1]==wf]
        specific_workflow.sort(key=lambda x: x[2])
        optimization_seq = []

        previous = specific_workflow[0][-1]
        optimization_seq.append(specific_workflow[0])
        for sw in specific_workflow[1:]:
            if sw[-1] == previous:
                if previous == "added":
                    continue
                else:
                    optimization_seq[-1] = sw
            else:
                previous = sw[-1]
                optimization_seq.append(sw)
        
        optimization_ts = []

        if len(optimization_ts) == 1:
            optimization_ts.append((repo_name, wf, optimization_seq[0][2], ref_temp))
        else:
            for i in range(len(optimization_seq)-1):
                if optimization_seq[i][-1]=="added":
                    optimization_ts.append((repo_name, wf, optimization_seq[i][2], optimization_seq[i+1][2]))
                else:
                    continue
            if optimization_seq[-1][-1]=="removed":
                pass
            elif optimization_seq[-1][-1]=="added":
                optimization_ts.append((repo_name, wf, optimization_seq[-1][2], ref_temp))
            
        if optimization_ts:
            optimization_history.extend(optimization_ts)

    return optimization_history

def get_fail_fast_ts(collected_commits):
    optimization_dict = get_optimization_usage(collected_commits, optimization="fail-fast:false")
    optimization_added = optimization_dict["optimization_added"]
    optimization_removed = optimization_dict["optimization_removed"]
    created_with_optimization = optimization_dict["created_with_optimization"]
    all_workflows = [(*tup, "added") for tup in optimization_added] + [(*tup, "removed") for tup in optimization_removed]
    upper_ref_temp = 1679310197
    lower_ref_temp = 1598918400
    optimization_history = []
    workflows_set = list(set([(ca[0], ca[1]) for ca in optimization_added]+[(cwc[0], cwc[1]) for cwc in created_with_optimization]))
    for i in range(len(workflows_set)):
        repo_name = workflows_set[i][0]
        wf = workflows_set[i][1]
        specific_workflow = [tup for tup in all_workflows if tup[0]==repo_name and tup[1]==wf]
        if len(specific_workflow) == 0:
            continue
        specific_workflow.sort(key=lambda x: x[2])
        optimization_seq = []
    
        previous = specific_workflow[0][-1]
        optimization_seq.append(specific_workflow[0])
        for sw in specific_workflow[1:]:
            if sw[-1] == previous:
                if previous == "added":
                    continue
                else:
                    optimization_seq[-1] = sw
            else:
                previous = sw[-1]
                optimization_seq.append(sw)
        
        optimization_ts = []

        if len(optimization_seq) == 1:
            optimization_ts.append((repo_name, wf, lower_ref_temp, optimization_seq[0][2]))
        elif len(optimization_seq) != 0:
            for i in range(1, len(optimization_seq)-1, 1):
                if optimization_seq[i][-1]=="removed":
                    optimization_ts.append((repo_name, wf, optimization_seq[i][2], optimization_seq[i+1][2]))
                else:
                    continue
            if optimization_seq[0][-1]=="removed":
                optimization_ts.append((repo_name, wf, optimization_seq[0][2], optimization_seq[1][2]))
            elif optimization_seq[0][-1]=="added":
                optimization_ts.append((repo_name, wf, lower_ref_temp, optimization_seq[0][2]))
            if optimization_seq[-1][-1]=="removed":
                optimization_ts.append((repo_name, wf, optimization_seq[-1][2], upper_ref_temp))

        if optimization_ts:
            optimization_history.extend(optimization_ts)

    return optimization_history

def get_optimization_ts_v2(collected_commits, optimization="cache@"):
    optimization_dict = get_optimization_usage(collected_commits, optimization=optimization)
    optimization_added = optimization_dict["optimization_added"]
    optimization_removed = optimization_dict["optimization_removed"]
    created_with_optimization = optimization_dict["created_with_optimization"]
    all_workflows = [(*tup, "removed") for tup in created_with_optimization] + [(*tup, "added") for tup in optimization_added] + [(*tup, "removed") for tup in optimization_removed]
    ref_temp = 1679310197
    optimization_history = []
    no_optimization_history = []
    workflows_set = list(set([(ca[0], ca[1]) for ca in optimization_added]+[(cwc[0], cwc[1]) for cwc in created_with_optimization]))
    for i in range(len(workflows_set)):
        repo_name = workflows_set[i][0]
        wf = workflows_set[i][1]
        specific_workflow = [tup for tup in all_workflows if tup[0]==repo_name and tup[1]==wf]
        if len(specific_workflow) == 0:
            continue
        specific_workflow.sort(key=lambda x: x[2])
        optimization_seq = []
        
        previous = specific_workflow[0][-1]
        
        optimization_seq.append(specific_workflow[0])
        for sw in specific_workflow[1:]:
            if sw[-1] == previous:
                if previous == "added":
                    continue
                else:
                    optimization_seq[-1] = sw
            else:
                previous = sw[-1]
                optimization_seq.append(sw)
        
        optimization_ts = []
        no_optimization_ts = []

        for wf in optimization_seq:
            if wf[-1] == "added":
                optimization_ts.append(wf)
            elif wf[-1] =="removed":
                no_optimization_ts.append(wf)
        if optimization_ts:
            optimization_history.append(optimization_ts)
        if no_optimization_ts:
            no_optimization_history.append(no_optimization_ts)

    return optimization_history, no_optimization_history


"""def get_optimization_time_improvement_cache(collected_commits, data_set, optimization="cache@"):
    optimization, no_optimization = get_optimization_ts(collected_commits, optimization=optimization)
    all_repos = data_set.get_all_repositories()
    all_runs = data_set.get_all_runs()
    all_jobs = data_set.get_all_jobs()
    all_runs["start_ts"] = all_runs.created_at.apply(lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    runs_repos = all_runs.merge(all_repos, left_on="repo_id", right_on="id")

    added_optimization_history = []
    removed_optimization_history = []
    
    optimization_unpacked = []
    for op in optimization:
        optimization_unpacked.extend(op)
        
    no_opt_unpacked = []
    for no_op in no_optimization:
        no_opt_unpacked.extend(no_op)
        
    for opt in optimization_unpacked:
        if len(opt) == 0 :
            continue
        runs_before = runs_repos[(runs_repos.full_name==opt[0]) & 
                       (runs_repos.workflow_file==".github/workflows/"+opt[1]) & 
                       (runs_repos.start_ts < opt[2])] 
        
        max_ts = runs_before.start_ts.max()
        runs_before = runs_before[runs_before.start_ts==max_ts]

        runs_after = runs_repos[(runs_repos.full_name==opt[0]) & 
                       (runs_repos.workflow_file==".github/workflows/"+opt[1]) & 
                       (runs_repos.start_ts >= opt[2])]
        min_ts = runs_after.start_ts.min()
        runs_after = runs_after[runs_after.start_ts==min_ts]

        ids_before = runs_before.id_x.to_list()
        ids_after = runs_after.id_x.to_list()

        non_optimized_jobs = all_jobs[all_jobs.run_id.isin(ids_before)]
        optimized_jobs = all_jobs[all_jobs.run_id.isin(ids_after)]

        mean_before = non_optimized_jobs.groupby("run_id").agg({"up_time": "sum"}).reset_index().up_time.mean()/60
        mean_after = optimized_jobs.groupby("run_id").agg({"up_time": "sum"}).reset_index().up_time.mean()/60

        if mean_before > mean_after:
            diff = -(mean_before-mean_after)/mean_before
        else:
            diff = (mean_after-mean_before)/mean_after
        
        if not np.isnan(diff):
            added_optimization_history.append((mean_before, mean_after, diff, ids_before, ids_after))

    for notopt in no_opt_unpacked:
        if len(notopt) == 0:
            continue
        runs_before = runs_repos[(runs_repos.full_name==notopt[0]) & 
                       (runs_repos.workflow_file==".github/workflows/"+notopt[1]) & 
                       (runs_repos.start_ts < notopt[2])] 
        
        max_ts = runs_before.start_ts.max()
        runs_before = runs_before[runs_before.start_ts==max_ts]

        runs_after = runs_repos[(runs_repos.full_name==notopt[0]) & 
                       (runs_repos.workflow_file==".github/workflows/"+notopt[1]) & 
                       (runs_repos.start_ts >= notopt[2])]
        min_ts = runs_after.start_ts.min()
        runs_after = runs_after[runs_after.start_ts==min_ts]

        ids_before = runs_before.id_x.to_list()
        ids_after = runs_after.id_x.to_list()

        non_optimized_jobs = all_jobs[all_jobs.run_id.isin(ids_after)]
        optimized_jobs = all_jobs[all_jobs.run_id.isin(ids_before)]

        mean_before = optimized_jobs.groupby("run_id").agg({"up_time": "sum"}).reset_index().up_time.mean()/60
        mean_after = non_optimized_jobs.groupby("run_id").agg({"up_time": "sum"}).reset_index().up_time.mean()/60

        if mean_before > mean_after:
            diff = (mean_before-mean_after)/mean_before
        else:
            diff = -(mean_after-mean_before)/mean_after
        if not np.isnan(diff):
            removed_optimization_history.append((mean_before, mean_after, diff, ids_after, ids_before))

    return added_optimization_history, removed_optimization_history"""

def get_optimization_time_improvement(collected_commits, data_set, optimization="cache@"):
    optimization_history = get_optimization_ts(collected_commits, optimization=optimization)
    all_repos = data_set.get_all_repositories()
    all_runs = data_set.get_all_runs()
    all_jobs = data_set.get_all_jobs()

    all_runs["start_ts"] = all_runs.created_at.apply(lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    runs_repos = all_runs.merge(all_repos, left_on="repo_id", right_on="id")
    
    optimized_runs_ids = []
    for c in optimization_history:
        optimized_runs_ids.extend(
            runs_repos[(runs_repos.full_name==c[0]) & 
                       (runs_repos.workflow_file==".github/workflows/"+c[1]) & 
                       (runs_repos.start_ts > c[2])& 
                       (runs_repos.start_ts < c[3])].id_x.to_list())


    optimized_jobs = all_jobs[all_jobs.run_id.isin(optimized_runs_ids)]
    
    return {
                "mean time for optimized runs": optimized_jobs.groupby("run_id").agg({"up_time": "sum"}).mean()/60, 
                "optimized runs ids": optimized_runs_ids,
    }

def get_fail_fast_time_impact(collected_commits, data_set):
    optimization_history = get_fail_fast_ts(collected_commits)
    all_repos = data_set.get_all_repositories()
    all_runs = data_set.get_all_runs()
    all_jobs = data_set.get_all_jobs()

    all_runs["start_ts"] = all_runs.created_at.apply(lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    runs_repos = all_runs.merge(all_repos, left_on="repo_id", right_on="id")
    
    time_impacts = []
    for c in optimization_history:
        optimized_runs_ids = []
        fail_fast_runs_ids = []
        estimated_runs_ids = []
        
        ids_to_add = runs_repos[(runs_repos.full_name==c[0]) & 
                       (runs_repos.workflow_file==".github/workflows/"+c[1]) & 
                       (runs_repos.start_ts > c[2])& 
                       (runs_repos.start_ts < c[3])&
                       (runs_repos.conclusion=="failure")].id_x.to_list()
        
        optimized_runs_ids.extend(ids_to_add)
        
        
        fail_fast_runs = runs_repos[(runs_repos.full_name==c[0]) & 
                           (runs_repos.workflow_file==".github/workflows/"+c[1]) & 
                           (runs_repos.start_ts > c[2])& 
                           (runs_repos.start_ts < c[3])&
                           (runs_repos.conclusion=="success")]
        
        fail_fast_runs_ids.extend(fail_fast_runs.id_x.to_list())
        
        for run_id in ids_to_add:
            run_ts = runs_repos[runs_repos.id_x == run_id]["start_ts"].to_list()[0]
            try:
                estimated_runs_ids.append(
                    fail_fast_runs[(fail_fast_runs.start_ts>run_ts)].sort_values("start_ts").id_x.to_list()[0])
            except Exception as e:
                estimated_runs_ids.append(
                    fail_fast_runs[(fail_fast_runs.start_ts<run_ts)].sort_values("start_ts", ascending=False).id_x.to_list()[0])
            
        optimized_jobs = all_jobs[all_jobs.run_id.isin(optimized_runs_ids)]
        fail_fast_jobs = all_jobs[all_jobs.run_id.isin(fail_fast_runs_ids)]


        estimated_jobs_time = []
        for run_id in estimated_runs_ids:
            estimated_jobs_time.append(all_jobs[all_jobs.run_id==run_id].up_time.sum())
    #estimated_jobs =  all_jobs[all_jobs.run_id.isin(estimated_runs_ids)]
    
        #print("optimized ids:", len(optimized_runs_ids))
        #print("fail fast ids:", len(fail_fast_runs_ids))
        #print("estimated jobs:", len(estimated_runs_ids))
        
        opt_fail = optimized_jobs.groupby("run_id").agg({"up_time": "sum"}).up_time.sum()
        success_time = fail_fast_jobs.groupby("run_id").agg({"up_time": "sum"}).up_time.sum()
        estimated_time = sum(estimated_jobs_time)
        if estimated_time+success_time != 0:
            time_impacts.append((estimated_time-opt_fail)/(estimated_time+success_time))
            if time_impacts[-1] < -1 or time_impacts[-1] > 1:
                print(estimated_time)
                print(opt_fail)
                print(success_time)
                print(c)
                del time_impacts[-1]
    #assert len(estimated_runs_ids)==len(optimized_runs_ids)
    return time_impacts

"""
def cancel_in_progress_impact(data_set, collected_commits):
    cancel_inprogress = get_optimization_ts(collected_commits, optimization="cancel-in-progress")
    all_runs = data_set.get_all_runs()
    all_jobs = data_set.get_all_jobs()
    all_repos = data_set.get_all_repositories()
    runs_total_time = all_jobs.groupby("run_id").agg({"up_time": "sum"}).reset_index()
    all_runs = all_runs.merge(runs_total_time, left_on="id", right_on="run_id")
    all_runs["start_ts"] = all_runs.created_at.apply(lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    runs_repos = all_runs.merge(all_repos[["id", "full_name"]], left_on="repo_id", right_on="id")
    optimized_runs = []
    possible_ids = []
    optimized_ids = []
    for cip in cancel_inprogress:
        possible_runs = runs_repos[(runs_repos.full_name==cip[0]) & (runs_repos.workflow_file==".github/workflows/"+cip[1]) & (runs_repos.start_ts>cip[2]) & (runs_repos.start_ts<cip[3])]
        possible_runs = possible_runs.sort_values("start_ts")
        possible_ids.extend(possible_runs.id_x.to_list())
        previous_canceled = False
        previous_run = None
        for i, row in possible_runs.iterrows():
            if previous_canceled:
                max_job_time = all_jobs[all_jobs.run_id == previous_run.id_x].up_time.max()
                if previous_run["start_ts"]+max_job_time<row["start_ts"]<previous_run["start_ts"]+max_job_time+60:
                    optimized_runs.append(row["up_time"] - previous_run["up_time"])
                    optimized_ids.append(row["id_x"])
            if row["conclusion"]== "cancelled":
                previous_canceled = True
                previous_run = row
            else:
                previous_canceled = False
    return optimized_runs, possible_ids, optimized_ids
"""

def cancel_in_progress_impact(data_set, repos_list, collected_commits):
    cancel_inprogress = get_optimization_ts(collected_commits, optimization="cancel-in-progress")
    all_runs = data_set.get_all_runs()
    all_runs = all_runs[all_runs.repo_id.isin(repos_list)]
    all_jobs = data_set.get_all_jobs()
    all_repos = data_set.get_all_repositories()
    runs_total_time = all_jobs.groupby("run_id").agg({"up_time": "sum"}).reset_index()
    all_runs = all_runs.merge(runs_total_time, left_on="id", right_on="run_id")
    all_runs["start_ts"] = all_runs.created_at.apply(lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    runs_repos = all_runs.merge(all_repos[["id", "full_name"]], left_on="repo_id", right_on="id")
    optimized_runs = []
    possible_ids = []
    optimized_ids = []
    for cip in cancel_inprogress:
        possible_runs = runs_repos[(runs_repos.full_name==cip[0]) & (runs_repos.workflow_file==".github/workflows/"+cip[1]) & (runs_repos.start_ts>cip[2]) & (runs_repos.start_ts<cip[3])]
        possible_runs = possible_runs.sort_values("start_ts")
        possible_ids.extend(possible_runs.id_x.to_list())
        previous_canceled = False
        previous_run = None
        for i, row in possible_runs.iterrows():
            if previous_canceled:
                max_job_time = all_jobs[all_jobs.run_id == previous_run.id_x].up_time.max()
                if previous_run["start_ts"]+max_job_time<row["start_ts"]<previous_run["start_ts"]+max_job_time+60:
                    optimized_runs.append(row["up_time"] - previous_run["up_time"])
                    optimized_ids.append(row["id_x"])
            if row["conclusion"]== "cancelled":
                previous_canceled = True
                previous_run = row
            else:
                previous_canceled = False
    return optimized_runs, possible_ids, optimized_ids

def calc_cancel_in_progress_impact(data_set, possible_ids, optimized_ids, saved_diff):
    
    all_jobs = data_set.get_all_jobs()
    all_runs = data_set.get_all_runs()
    runs_total_time = all_jobs.groupby("run_id").agg({"up_time": "sum"}).reset_index()
    all_runs = all_runs.merge(runs_total_time, left_on="id", right_on="run_id")
    all_runs["start_ts"] = all_runs.created_at.apply(lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    
    possible_ids_time = all_runs[all_runs.id.isin(possible_ids)].up_time.sum()
    saved_time = sum(saved_diff)
    saved_cost = saved_time*1.52*0.008/60
    possible_runs = all_runs[all_runs.id.isin(possible_ids)]
    min_max_start_ts = possible_runs.groupby("repo_id").start_ts.agg(["min", "max"]).reset_index()
    total_start_ts = 0
    for i, row in min_max_start_ts.iterrows():
        total_start_ts += row["max"] - row["min"]
    years = total_start_ts/(12*30*24*3600)
    return saved_time/(possible_ids_time+saved_time)*100, len(optimized_ids)/len(possible_ids)*100, saved_cost/years


"""def calc_skip_impact(data_set, commits_dict):
    
    all_repos = data_set.get_all_repositories()
    all_jobs = data_set.get_all_jobs()
    all_runs = data_set.get_all_runs()
    all_runs["start_ts"] = all_runs.created_at.apply(lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    runs_total_time = all_jobs.groupby("run_id").agg({"up_time": "sum"}).reset_index()
    all_runs = all_runs.merge(runs_total_time, left_on="id", right_on="run_id")
    runs_repos = all_runs.merge(all_repos[["id", "full_name"]], left_on="repo_id", right_on="id")
    
    skipped_prop = []
    skipped_repos = []
    for repo in commits_dict:
        skipped_commits = []
        for fc in commits_dict[repo]:
            if any(x in fc for x in ["skip ci", "ci skip", "no ci", "skip actions", "actions skip", "skip-checks:true", "skip-checks: true"]):
                skipped_commits.append(fc)
        skipped_prop.append(len(skipped_commits))
        if len(skipped_commits)!=0:
            skipped_repos.append(repo)
    
    impacted_repos_prop = len([x for x in skipped_prop if x!=0])/len(collected_commits)*100
    impacted_runs_prop = sum(skipped_prop)/all_runs.shape[0]*100
    
    skipped_repos_runs = runs_repos[runs_repos.full_name.isin(skipped_repos)]
    mean_skipped_time = skipped_repos_runs.groupby("full_name").agg({"up_time": "mean"}).reset_index()
    total_time = 0
    years_time = 0
    for repo, n_runs  in zip(skipped_repos, [x for x in skipped_prop if x!=0]):
        total_time += mean_skipped_time[mean_skipped_time.full_name==repo].up_time.to_list()[0] * n_runs
        years_time += runs_repos[runs_repos.full_name==repo].start_ts.max() - runs_repos[runs_repos.full_name==repo].start_ts.min()
        
    impact_vm_time = total_time/runs_repos.up_time.sum() * 100
    years = years_time / (12*30*24*3600)
    delta_cost = total_time/years / 60 * 1.52 * 0.008
    
    return impacted_repos_prop*100, impacted_runs_prop*100, impact_vm_time*100, delta_cost
"""

def calc_skip_impact(data_set, repos_list, commits_dict):
    
    all_repos = data_set.get_all_repositories()
    all_jobs = data_set.get_all_jobs()
    all_runs = data_set.get_all_runs()
    all_runs = all_runs[all_runs.repo_id.isin(repos_list)]
    all_runs["start_ts"] = all_runs.created_at.apply(lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    runs_total_time = all_jobs.groupby("run_id").agg({"up_time": "sum"}).reset_index()
    all_runs = all_runs.merge(runs_total_time, left_on="id", right_on="run_id")
    runs_repos = all_runs.merge(all_repos[["id", "full_name"]], left_on="repo_id", right_on="id")
    updated_repos_list = runs_repos[(runs_repos.full_name.isin(list(commits_dict.keys())))]
    skipped_prop = []
    skipped_repos = []
    for repo in commits_dict:
        skipped_commits = []
        for fc in commits_dict[repo]:
            if any(x in fc for x in ["skip ci", "ci skip", "no ci", "skip actions", "actions skip", "skip-checks:true", "skip-checks: true"]):
                skipped_commits.append(fc)
        skipped_prop.append(len(skipped_commits))
        if len(skipped_commits)!=0:
            if repo in runs_repos.full_name.unique():
                skipped_repos.append(repo)
    
    impacted_repos_prop = len(skipped_repos)/len(repos_list) * 901 / 952 # number of repos for which messages where retrieved without errors
    impacted_runs_prop = sum(skipped_prop)/all_runs.shape[0]
    
    skipped_repos_runs = runs_repos[runs_repos.full_name.isin(skipped_repos)]
    mean_skipped_time = skipped_repos_runs.groupby("full_name").agg({"up_time": "mean"}).reset_index()
    total_time = 0
    years_time = 0
    for repo, n_runs  in zip(skipped_repos, [x for x in skipped_prop if x!=0]):
        total_time += mean_skipped_time[mean_skipped_time.full_name==repo].up_time.to_list()[0] * n_runs if mean_skipped_time[mean_skipped_time.full_name==repo].up_time.shape[0] != 0 else 0 
        years_time += runs_repos[runs_repos.full_name==repo].start_ts.max() - runs_repos[runs_repos.full_name==repo].start_ts.min()
        
    impact_vm_time = total_time/runs_repos.up_time.sum()
    years = years_time / (12*30*24*3600)
    delta_cost = total_time/years / 60 * 1.52 * 0.008
    
    return impacted_repos_prop*100, impacted_runs_prop*100, impact_vm_time*100, delta_cost

def get_cache_ts(collected_commits, version="cache@v"):
    optimization_dict = get_optimization_usage(collected_commits, optimization=version)
    optimization_added = optimization_dict["optimization_added"]
    optimization_removed = optimization_dict["optimization_removed"]
    created_with_optimization = optimization_dict["created_with_optimization"]
    all_workflows = [(*tup, "added") for tup in optimization_added] + [(*tup, "removed") for tup in optimization_removed]
    upper_ref_temp = 1679310197
    lower_ref_temp = 1598918400
    optimization_history = []
    workflows_set = list(set([(ca[0], ca[1]) for ca in optimization_added]+[(cwc[0], cwc[1]) for cwc in created_with_optimization]))
    for i in range(len(workflows_set)):
        repo_name = workflows_set[i][0]
        wf = workflows_set[i][1]
        specific_workflow = [tup for tup in all_workflows if tup[0]==repo_name and tup[1]==wf]
        if len(specific_workflow) == 0:
            continue
        specific_workflow.sort(key=lambda x: x[2])
        optimization_seq = []
    
        previous = specific_workflow[0][-1]
        optimization_seq.append(specific_workflow[0])
        for sw in specific_workflow[1:]:
            if sw[-1] == previous:
                if previous == "added":
                    continue
                else:
                    optimization_seq[-1] = sw
            else:
                previous = sw[-1]
                optimization_seq.append(sw)
        
        optimization_ts = []

        if len(optimization_seq) == 1:
            optimization_ts.append((repo_name, wf, lower_ref_temp, optimization_seq[0][2]))
        elif len(optimization_seq) != 0:
            for i in range(1, len(optimization_seq)-1, 1):
                if optimization_seq[i][-1]=="removed":
                    optimization_ts.append((repo_name, wf, optimization_seq[i][2], optimization_seq[i+1][2]))
                else:
                    continue
            if optimization_seq[0][-1]=="removed":
                optimization_ts.append((repo_name, wf, optimization_seq[0][2], optimization_seq[1][2]))
            elif optimization_seq[0][-1]=="added":
                optimization_ts.append((repo_name, wf, lower_ref_temp, optimization_seq[0][2]))
            if optimization_seq[-1][-1]=="removed":
                optimization_ts.append((repo_name, wf, optimization_seq[-1][2], upper_ref_temp))

        if optimization_ts:
            optimization_history.extend(optimization_ts)

    return optimization_history

"""def get_optimization_time_improvement_cache(collected_commits, data_set, repos_list, optimization="cache@"):
    optimization = get_cache_ts(collected_commits, version=optimization)
    all_repos = data_set.get_all_repositories()
    all_runs = data_set.get_all_runs()
    all_runs = all_runs[all_runs.repo_id.isin(repos_list)]
    all_jobs = data_set.get_all_jobs()
    all_runs["start_ts"] = all_runs.created_at.apply(lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    runs_repos = all_runs.merge(all_repos, left_on="repo_id", right_on="id")

    added_optimization_history = []
    removed_optimization_history = []
    
    runs_between = []
    for opt in optimization:
        if len(opt) == 0 :
            continue
        runs_before = runs_repos[(runs_repos.full_name==opt[0]) & 
                       (runs_repos.workflow_file==".github/workflows/"+opt[1]) & 
                       (runs_repos.start_ts < opt[2])] 
        
        max_ts = runs_before.start_ts.max()
        runs_before = runs_before[runs_before.start_ts==max_ts]

        runs_after = runs_repos[(runs_repos.full_name==opt[0]) & 
                       (runs_repos.workflow_file==".github/workflows/"+opt[1]) & 
                       (runs_repos.start_ts >= opt[2])]
        
        runs_between.extend(runs_repos[(runs_repos.start_ts>=opt[2])&(runs_repos.start_ts<=opt[3])].id_x.to_list())
        
        min_ts = runs_after.start_ts.min()
        runs_after = runs_after[runs_after.start_ts==min_ts]

        ids_before = runs_before.id_x.to_list()
        ids_after = runs_after.id_x.to_list()

        non_optimized_jobs = all_jobs[all_jobs.run_id.isin(ids_before)]
        optimized_jobs = all_jobs[all_jobs.run_id.isin(ids_after)]

        mean_before = non_optimized_jobs.groupby("run_id").agg({"up_time": "sum"}).reset_index().up_time.mean()/60
        mean_after = optimized_jobs.groupby("run_id").agg({"up_time": "sum"}).reset_index().up_time.mean()/60

        if mean_before > mean_after:
            diff = -(mean_before-mean_after)/mean_before
        else:
            diff = (mean_after-mean_before)/mean_after
        
        if not np.isnan(diff):
            added_optimization_history.append((mean_before, mean_after, diff, ids_before, ids_after))

    return added_optimization_history, runs_between
"""

def calc_cache_impact(new_collected_commits, data_set, repos_list):
    added_optimization, runs_between = get_optimization_time_improvement_cache(new_collected_commits, 
                                                                           data_set, 
                                                                           repos_list)
    average_time_impact = float(str(sum([x[2] for x in added_optimization])/len(added_optimization))[:5])
    all_runs = data_set.get_all_runs()
    possible_runs = all_runs[all_runs.id.isin(runs_between)]
    possible_runs["start_ts"] = possible_runs.created_at.apply(lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    min_max_start_ts = possible_runs.groupby("repo_id").start_ts.agg(["min", "max"]).reset_index()
    total_start_ts = 0
    for i, row in min_max_start_ts.iterrows():
        total_start_ts += row["max"] - row["min"]
    years = total_start_ts/(12*30*24*3600)
    all_jobs = data_set.get_all_jobs()
    cost_delta = all_jobs[all_jobs.run_id.isin(possible_runs.id)].up_time.sum()/60/years * average_time_impact * 1.52 * 0.008
    return average_time_impact, cost_delta

def get_optimization_time_improvement_cache(collected_commits, data_set, repos_list, optimization="cache@v2"):
    optimization = get_cache_ts(collected_commits, version=optimization)
    all_repos = data_set.get_all_repositories()
    all_runs = data_set.get_all_runs()
    all_runs = all_runs[all_runs.repo_id.isin(repos_list)]
    all_jobs = data_set.get_all_jobs()
    all_runs["start_ts"] = all_runs.created_at.apply(lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())))
    runs_repos = all_runs.merge(all_repos, left_on="repo_id", right_on="id")

    added_optimization_history = []
    removed_optimization_history = []
    
    runs_between = []
    for opt in optimization:
        if len(opt) == 0 :
            continue
        runs_before = runs_repos[(runs_repos.full_name==opt[0]) & 
                       (runs_repos.workflow_file==".github/workflows/"+opt[1]) & 
                       (runs_repos.start_ts < opt[2])] 
        
        max_ts = runs_before.start_ts.max()
        runs_before = runs_before[runs_before.start_ts==max_ts]

        runs_after = runs_repos[(runs_repos.full_name==opt[0]) & 
                       (runs_repos.workflow_file==".github/workflows/"+opt[1]) & 
                       (runs_repos.start_ts >= opt[2])]
        
        runs_between.extend(runs_repos[(runs_repos.start_ts>=opt[2])&(runs_repos.start_ts<=opt[3])].id_x.to_list())
        
        min_ts = runs_after.start_ts.min()
        runs_after = runs_after[runs_after.start_ts==min_ts]

        ids_before = runs_before.id_x.to_list()
        ids_after = runs_after.id_x.to_list()

        non_optimized_jobs = all_jobs[all_jobs.run_id.isin(ids_before)]
        optimized_jobs = all_jobs[all_jobs.run_id.isin(ids_after)]

        mean_before = non_optimized_jobs.groupby("run_id").agg({"up_time": "sum"}).reset_index().up_time.mean()/60
        mean_after = optimized_jobs.groupby("run_id").agg({"up_time": "sum"}).reset_index().up_time.mean()/60

        if mean_before > mean_after:
            diff = -(mean_before-mean_after)/mean_before
        else:
            diff = (mean_after-mean_before)/mean_after
        
        if not np.isnan(diff):
            added_optimization_history.append((mean_before, mean_after, diff, ids_before, ids_after))

    return added_optimization_history, runs_between

def get_optimization_usage_avm(collected_commits, optimization="cache@"):
    optimization_added = []
    optimization_removed =  []
    created_with_optimization = []
    for item in collected_commits:
        for mdfile in item["Modified"]:
            #pprint(mdfile.export_as_dict())
            file_name = mdfile["file_name"]
            added_lines = [c[1] for c in mdfile["diff_parsed"]["added"]]
            removed_lines = [c[1] for c in mdfile["diff_parsed"]["deleted"]]
            added = False
            for al in added_lines:
                if optimization in al.lower().replace(" ", "").replace("\t", "") and not al.lower().replace(" ", "").replace("\t", "").startswith("#"):
                    optimization_added.append((mdfile["repo"], 
                                               file_name, 
                                               int(mdfile["c_date"]), 
                                              int(al.lower().replace(" ", "").replace("\t", "").replace("timeout-minutes:", "")),
                                              ))
                    added = True
                    break
            if not added:
                for rl in removed_lines:
                    if optimization in rl.lower().replace(" ", "").replace("\t", "") and not al.lower().replace(" ", "").replace("\t", "").startswith("#"):
                        try:
                            optimization_removed.append((mdfile["repo"], 
                                                         file_name, 
                                                         int(mdfile["c_date"])
                                                        ))
                        except:
                            print("error")
        for c_file in item["Added"]:
            file_name = c_file["file_name"]
            #pprint(c_file.export_as_dict())
            added_lines = [c[1] for c in c_file["diff_parsed"]["added"]]
            for al in added_lines:
                if optimization in al.lower().replace(" ", "").replace("\t", "") and not al.lower().replace(" ", "").replace("\t", "").startswith("#"):
                    created_with_optimization.append((c_file["repo"], 
                                                      file_name, 
                                                      int(c_file["c_date"]),
                                                     int(al.lower().replace(" ", "").replace("\t", "").replace("timeout-minutes:", ""))))

    return {    
        "optimization_added": optimization_added, 
        "optimization_removed": optimization_removed, 
        "created_with_optimization": created_with_optimization
    }

def get_optimization_ts_avm(collected_commits, optimization="cache@"):
    optimization_dict = get_optimization_usage_avm(collected_commits, optimization=optimization)
    optimization_added = optimization_dict["optimization_added"]
    optimization_removed = optimization_dict["optimization_removed"]
    created_with_optimization = optimization_dict["created_with_optimization"]
    all_workflows = [(*tup, "added") for tup in optimization_added] + [(*tup, "removed") for tup in optimization_removed] + [(*tup, "added") for tup in created_with_optimization]
    ref_temp = 1679310197
    optimization_history = []
    workflows_set = list(set([(ca[0], ca[1]) for ca in optimization_added]+[(cwc[0], cwc[1]) for cwc in created_with_optimization]))
    for i in range(len(workflows_set)):
        repo_name = workflows_set[i][0]
        wf = workflows_set[i][1]
        specific_workflow = [tup for tup in all_workflows if tup[0]==repo_name and tup[1]==wf]
        specific_workflow.sort(key=lambda x: x[2])
        optimization_seq = []

        previous = specific_workflow[0][-1]
        optimization_seq.append(specific_workflow[0])
        for sw in specific_workflow[1:]:
            if sw[-1] == previous:
                if previous == "added":
                    continue
                else:
                    optimization_seq[-1] = sw
            else:
                previous = sw[-1]
                optimization_seq.append(sw)
        
        optimization_ts = []

        if len(optimization_ts) == 1:
            optimization_ts.append((repo_name, wf, optimization_seq[0][2], ref_temp))
        else:
            for i in range(len(optimization_seq)-1):
                if optimization_seq[i][-1]=="added":
                    optimization_ts.append((repo_name, wf, optimization_seq[i][2], optimization_seq[i+1][2], optimization_seq[i][3]))
                else:
                    continue
            if optimization_seq[-1][-1]=="removed":
                pass
            elif optimization_seq[-1][-1]=="added":
                optimization_ts.append((repo_name, wf, optimization_seq[-1][2], ref_temp, optimization_seq[-1][3]))
            
        if optimization_ts:
            optimization_history.extend(optimization_ts)

    return optimization_history