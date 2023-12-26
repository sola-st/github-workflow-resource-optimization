def names_mapping(name):
    if "test" in name:
        return "test"
    if "build" in name:
        return "build"
    if "deploy" in name:
        return "deploy"
    if "mutate" in name:
        return "mutate"
    if "coverage" in name:
        return "coverage"
    if "lint" in name:
        return "lint"
    if "checksum" in name:
        return "checksum"
    if "publish" in name:
        return "publish"
    if "release" in name:
        return "release"
    if "integration" in name:
        return "integration"
    if "sync" in name:
        return "sync"
    if "production" in name:
        return "production"
    if "setup" in name:
        return "setup"
    if "update" in name:
        return "update"
    return name

def simplify_and_map(data_set, repos_list):
    all_runs = data_set.get_all_runs()
    all_runs = all_runs[all_runs.repo_id.isin(repos_list)]
    all_jobs = data_set.get_all_jobs()
    all_jobs = all_jobs[all_jobs.run_id.isin(all_runs.id)]
    all_jobs["simple_name"] = all_jobs.name.apply(lambda x: x[:x.find("(")].replace(" ", "").lower() if "(" in x else x.replace(" ", "").lower())
    all_jobs["sub_names"] = all_jobs.simple_name.apply(lambda x: names_mapping(x))
    return all_jobs

def get_topk_jobs_names(data_set, repos_list, k=10):
    all_jobs = simplify_and_map(data_set, repos_list)
    names_count = all_jobs.groupby("sub_names").sub_names.agg(["count"])
    top_k = names_count.sort_values("count", ascending=False)[:k]
    top_k["job_name"] = top_k.index
    return top_k

def get_topk_proportions(data_set, k=10):
    top_k = get_topk_jobs_names(data_set, k=k)
    return top_k["count"]/top_k["count"].sum()*100

def get_topk_jobs_time(data_set, k=10):
    all_jobs = data_set.get_all_jobs()
    top_k = get_topk_jobs_names(data_set, k=k)
    top_tasks_jobs = all_jobs[all_jobs.sub_names.isin(top_k.job_name)]
    top_tasks_jobs["up_time_min"] = top_tasks_jobs.up_time/60
    sum_time_tasks = top_tasks_jobs.groupby("sub_names").up_time.agg(["sum"]).reset_index("sub_names")
    sum_time_tasks["prop"] = sum_time_tasks["sum"]/sum_time_tasks["sum"].sum()*100
    return sum_time_tasks