import pandas as pd
import json
import time
import datetime
from pathlib import Path
#from pandas_profiling import ProfileReport

class Table():
    def __init__(self, table_path):
        with open(table_path) as tp:
            table = json.load(tp)
        self.df = pd.DataFrame(table)

    def get_shape(self):
        return self.df.shape

    def get_columns(self):
        return self.df.columns

    def to_html(self, path):
        self.df.to_html(path)
class RunsTable(Table):

    def __init__(self, table_path):
        Table.__init__(self, table_path)
        #print("runs shape:", self.df.shape)
        self.df.drop_duplicates(subset=["id"], inplace=True)

    def get_branches(self):
        return self.df.branch.unique().to_list()

    def get_workflows(self):
        return self.df.workflow_file.unique().to_list()

    def get_repo_ids(self):
        return self.df.repo_id.unique().to_list()

    def get_avg_jobs(self):
        return self.df.total_count.mean()

    def get_med_jobs(self):
        return self.df.total_count.median()

    def get_events_list(self):
        return self.df.event.unique().to_list()

    def to_html(self, path):
        self.df.to_html(path)


class ActorsTable(Table):
    def __init__(self, table_path):
        Table.__init__(self, table_path)
        self.df.drop_duplicates(inplace=True)

    def get_actors(self):
        return self.df.login.to_list()

    def get_actors_ids(self):
        return self.df.id.to_list()

class CommitsTable(Table):
    def __init__(self, table_path):
        Table.__init__(self, table_path)
        self.df.drop_duplicates(inplace=True)

class JobsTable(Table):
    def __init__(self, table_path):
        Table.__init__(self, table_path)
        self.df.drop_duplicates(inplace=True)
        self.df["start_ts"] = self.df.started_at.apply(lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())) if x is not None else -1)
        self.df["end_ts"] = self.df.completed_at.apply(lambda x: int(time.mktime(datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ").timetuple())) if x is not None else -1)
        self.df["up_time"] = self.df.end_ts - self.df.start_ts
        self.df["up_time"] = self.df.up_time.apply(lambda x: x if x > 0 else 0)

    def get_successful(self):
        return self.df[self.df.conclusion == "success"]

    def get_failed(self):
        return self.df[self.df.conclusion == "failure"]

    def get_cancelled(self):
        return self.df[self.df.conclusion == "cancelled"]

    def get_completed(self):
        return self.df[self.df.status=="completed"]

    def get_status_dist(self, n = 10):
        return self.df.groupby("status").aggregate({"status": "count"}["status"])

    def get_queued(self):
        pass

    def get_conclusion_dist(self):
        return self.df.groupby("conclusion").aggregate({"conclusion": "count"}["conclusion"])
    
    def get_total_runtime(self):
        return self.df.up_time.sum()

    def get_time_by_run(self):
        return self.df.groupby("run_id").aggregate({"up_time":"sum"})

    def get_mean_runtime(self):
        return self.df.groupby("run_id").aggregate({"up_time":"sum"}).mean()

    def get_jobs_between_timestamps(self, start_ts, end_ts):
        return self.df[(self.df.start_ts >= start_ts) & (self.df.end_ts <= end_ts)]

    def get_runtime_between_timestamps(self, start_ts, end_ts):
        return self.df[(self.df.start_ts >= start_ts) & (self.df.end_ts <= end_ts)].up_time.sum()

    """def pandas_profiling(self, save_to="./report.html"):
        profile = ProfileReport(self.df[["status", "conclusion", "up_time", "start_ts", "end_ts"]], title="Pandas Profiling Report")
        profile.to_file(save_to)"""

class RepositoryTable(Table):
    def __init__(self, table_path):
        Table.__init__(self, table_path)
        self.df.drop_duplicates(inplace=True)

class PullRequestsTable(Table):
    def __init__(self, table_path):
        Table.__init__(self, table_path)
        self.df.drop_duplicates(inplace=True)

class StepsTable(Table):
    def __init__(self, table_path):
        Table.__init__(self, table_path)
        self.df.drop_duplicates(inplace=True)

class RepoRuns():
    def __init__(self, paths):
        self.actors = ActorsTable(paths["actors_path"])
        self.commits = CommitsTable(paths["commits_path"])
        self.jobs = JobsTable(paths["jobs_path"])
        self.pull_requests = PullRequestsTable(paths["pull_requests_path"])
        self.repositories = RepositoryTable(paths["repositories_path"])
        self.runs = RunsTable(paths["runs_path"])
        self.steps = StepsTable(paths["steps_path"])
        self.repo_name = paths["repo_name"]

    def to_html(self, base = "./html_files/"):
        Path(base+self.repo_name).mkdir(parents=True, exist_ok=True)
        print("writing actors table to:", base + self.repo_name + "/" + "actors_table.html")
        self.actors.to_html(base+self.repo_name + "/" + "actors_table.html")
        print("writing commits table to:", base + self.repo_name + "/" + "commits_table.html")
        self.commits.to_html(base + self.repo_name + "/" + "commits_table.html")
        print("writing jobs table to:", base + self.repo_name + "/" + "jobs_table.html")
        self.jobs.to_html(base+self.repo_name + "/" + "jobs_table.html")
        print("writing pull_requests table to:", base + self.repo_name + "/" + "pull_requests_table.html")
        self.pull_requests.to_html(base + self.repo_name + "/" + "pull_requests_table.html")
        print("writing repositories table to:", base + self.repo_name + "/" + "repositories_table.html")
        self.repositories.to_html(base + self.repo_name + "/" + "repositories_table.html")
        print("writing runs table to:", base + self.repo_name + "/" + "runs_table.html")
        self.runs.to_html(base + self.repo_name + "/" + "runs_table.html")
        print("writing steps table to:", base + self.repo_name + "/" + "steps_table.html")
        self.steps.to_html(base + self.repo_name + "/" + "steps_table.html")

    def get_repo_summary(self):
        total_runs = self.runs.df.shape[0]
        total_jobs = self.runs.df.total_count.sum()
        total_steps = self.steps.df.shape[0]
        total_time = self.jobs.df.up_time.sum()
        avg_runtime = total_time / total_runs
        total_success = self.runs.df[self.runs.df.conclusion=="success"].shape[0]
        total_fail = self.runs.df[self.runs.df.conclusion=="failure"].shape[0]
        other_conclusion = self.runs.df[~self.runs.df.conclusion.isin(["success", "failure"])].shape[0]
        other_conclusion_list = self.runs.df[~self.runs.df.conclusion.isin(["success", "failure"])].conclusion.unique().tolist()
        pr_event = self.runs.df[self.runs.df.event == "pull_request"].shape[0]
        push_event = self.runs.df[self.runs.df.event == "push"].shape[0]
        other_event = self.runs.df[~self.runs.df.event.isin(["pull_request", "push"])].shape[0]
        other_event_list = self.runs.df[~self.runs.df.event.isin(["pull_request", "push"])].event.unique().tolist()
        first_run  = self.jobs.df.start_ts.min()
        last_run = max(self.jobs.df.end_ts.max(), self.jobs.df.start_ts.max())
        
        # TODO: add others list 
        return {
            "repo_name": self.repo_name.replace("#####", "/"),
            "total_runs": total_runs,
            "total_jobs": total_jobs,
            "total_steps": total_steps,
            "total_time": total_time,
            "avg_runtime": avg_runtime,
            "total_success": total_success,
            "total_fail": total_fail,
            "other_conclusion": other_conclusion,
            "other_conclusion_list": other_conclusion_list, 
            "pr_event": pr_event,
            "push_event": push_event,
            "other_event": other_event,
            "other_event_list": other_event_list,
            "first_run": first_run,
            "last_run": last_run
        }
    def get_total_runtime(self):
        return self.jobs.get_total_runtime()

    def get_time_by_run(self):
        return self.jobs.get_time_by_run()

    def get_mean_runtime(self):
        return self.jobs.get_mean_runtime()

    def get_jobs_between_two_timestamps(self, start_ts, end_ts):
        return self.jobs.get_jobs_between_timestamps(start_ts, end_ts)

    def get_runtime_between_timestamps(self, start_ts, end_ts):
        return self.jobs.get_runtime_between_timestamps(start_ts, end_ts)

    def export_data_set(self):
        pass

if __name__ == "main":
    repo_name = "ghosh#####micromodal"
    base_dir = "./tables/" + repo_name + "/"

    paths = {
        "repo_name": repo_name,
        "actors_path": "{0}{1}_actors_table.json".format(base_dir, repo_name),
        "commits_path": "{0}{1}_commits_table.json".format(base_dir, repo_name),
        "jobs_path": "{0}{1}_jobs_table.json".format(base_dir, repo_name),
        "pull_requests_path": "{0}{1}_pull_requests_table.json".format(base_dir, repo_name),
        "repositories_path": "{0}{1}_repositories_table.json".format(base_dir, repo_name),
        "runs_path": "{0}{1}_runs_table.json".format(base_dir, repo_name),
        "steps_path": "{0}{1}_steps_table.json".format(base_dir, repo_name)
    }

    repo_runs = RepoRuns(paths)
    #print(repo_runs.jobs.pandas_profiling())
    #df  = table_to_df("tables/sola-st#####DynaPyt_steps_table.json")
    #print(df.groupby("conclusion").aggregate({"conclusion": "count"})["conclusion"])
    #df.to_html("steps.html")

    #df["conclusion"].hist().figure.savefig("saved_plot.png")



#s = "2022-12-09T11:50:59Z"
#print(time.mktime(datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").timetuple()))