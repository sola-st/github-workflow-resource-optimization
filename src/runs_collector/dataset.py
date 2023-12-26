from runs_collector.tables import RepoRuns
import pandas as pd
import os

class RunsDataSet():
    def __init__(self, repositories_list, tables_dir, from_checkpoint=False, checkpoint_dir=None):
        self.repo_runs_list = []
        self.all_runs = None
        self.all_jobs = None
        self.all_commits = None
        self.all_actors = None
        self.all_steps = None
        self.all_repositories = None
        self.all_pull_requests = None
        
        if not from_checkpoint:
            print("Loading dataset from origianl files")
            for repo_name in repositories_list:
                base_dir = tables_dir + repo_name + "/"
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
                try:
                    repo_runs = RepoRuns(paths)
                    self.repo_runs_list.append(repo_runs)
                except Exception as e:
                    #an exception is expected because some data tables are empty and so cannot be dataframed
                    pass
                    #print(e, repo_name)
        else:
            print("Loading dataset from checkpoints")
            self.all_runs = pd.read_csv(os.path.join(checkpoint_dir, "all_runs.csv"))
            self.all_jobs = pd.read_csv(os.path.join(checkpoint_dir, "all_jobs.csv"))
            self.all_commits = pd.read_csv(os.path.join(checkpoint_dir, "all_commits.csv"))
            self.all_actors = pd.read_csv(os.path.join(checkpoint_dir, "all_actors.csv"))
            self.all_steps = pd.read_csv(os.path.join(checkpoint_dir, "all_steps.csv"))
            self.all_repositories = pd.read_csv(os.path.join(checkpoint_dir, "all_repositories.csv"))
            self.all_pull_requests = pd.read_csv(os.path.join(checkpoint_dir, "all_pull_requests.csv"))

    def size(self):
        return len(self.repo_runs_list)
    
    def get_all_runs(self):
        if self.all_runs is None:
            self.all_runs = pd.concat([repo_rl.runs.df for repo_rl in self.repo_runs_list]).drop_duplicates(subset=["id"])
            return self.all_runs
        else:
            return self.all_runs

    def get_all_jobs(self):
        if self.all_jobs is None:
            self.all_jobs = pd.concat([repo_rl.jobs.df for repo_rl in self.repo_runs_list]).drop_duplicates(subset=["id"])
            return self.all_jobs
        else:
            return self.all_jobs

    def get_all_repositories(self):
        if self.all_repositories is None:
            self.all_repositories = pd.concat([repo_rl.repositories.df for repo_rl in self.repo_runs_list]).drop_duplicates(subset=["id"])
            return self.all_repositories
        else:
            return self.all_repositories

    def get_all_commits(self):
        if self.all_commits is None:
            self.all_commits = pd.concat([repo_rl.commits.df for repo_rl in self.repo_runs_list]).drop_duplicates(subset=["id"])
            return self.all_commits
        else:
            return self.all_commits

    def get_all_pull_requests(self):
        if self.all_pull_requests is None:
            self.all_pull_requests = pd.concat([repo_rl.pull_requests.df for repo_rl in self.repo_runs_list]).drop_duplicates(subset=["id"])
            return self.all_pull_requests
        else:
            return self.all_pull_requests

    def get_all_steps(self):
        if self.all_steps is None:
            self.all_steps = pd.concat([repo_rl.steps.df for repo_rl in self.repo_runs_list]).drop_duplicates(subset=["number", "job_id"])
            return self.all_steps
        else:
            return self.all_steps

    def get_all_actors(self):
        if self.all_actors is None:
            self.all_actors = pd.concat([repo_rl.actors.df for repo_rl in self.repo_runs_list]).drop_duplicates(subset=["id"])
            return self.all_actors
        else:
            return self.all_actors


    def export_dataset(self, path):    
        self.get_all_runs().to_csv(os.path.join(path, "all_runs.csv"))
        self.get_all_jobs().to_csv(os.path.join(path, "all_jobs.csv"))
        self.get_all_commits().to_csv(os.path.join(path, "all_commits.csv"))
        self.get_all_pull_requests().to_csv(os.path.join(path, "all_pull_steps.csv"))
        self.get_all_steps().to_csv(os.path.join(path, "all_steps.csv"))
        self.get_all_actors().to_csv(os.path.join(path, "all_actors.csv"))
        self.get_all_repositories().to_csv(os.path.join(path, "all_repositories.csv"))
            