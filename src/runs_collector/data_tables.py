import json
from pathlib import Path
import os
from runs_collector.runs_scraper import get_jobs
import logging
import time

with open("tokens.json") as tk:
    tokens = json.load(tk)

logging.basicConfig(filename="collect_runs_tables_logs.log",
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)
    
def run_to_tables(run, token):
    jobs_url = run["jobs_url"]
    success = False
    while not success:
        try:
            jobs = get_jobs(jobs_url, token)
            #print(jobs)
            success = True
        except Exception as e:
            print("ERROR:", e)
            time.sleep(1)
    run_table = {
            "id": run["id"],
            "branch": run["head_branch"],
            "workflow_name": run["name"],
            "workflow_file": run["path"],
            "run_number": run["run_number"],
            "event": run["event"],
            "status": run["status"],
            "conclusion": run["conclusion"],
            "workflow_id": run["workflow_id"],
            "check_suite_id": run["check_suite_id"],
            "url": run["url"],
            "created_at": run["created_at"],
            "updated_at": run["updated_at"],
            "actor_id": run["actor"]["id"],
            "run_attempt": run["run_attempt"],
            "referenced_workflows": run["referenced_workflows"],
            "triggering_actor_id": run.get("triggering_actor", {"id": -1})["id"],
            "head_commit_id": run.get("head_commit", {"id": -1})["id"],
            "repo_id": run["repository"]["id"],
            "head_repo_id": run.get("head_repository", {"id": -1})["id"] if run.get("head_repository", {"id": -1}) is not None else -1,
            "total_count": jobs["total_count"]
        }

    jobs_table = []
    steps_table = []

    for job in jobs["jobs"]:
        job_table = {
            "id": job["id"],
            "run_id": run["id"],
            "run_attempt": job["run_attempt"],
            "status": job["status"],
            "conclusion": job["conclusion"],
            "name":job["name"],
            "started_at": job["started_at"],
            "completed_at": job["completed_at"]
        }
        jobs_table.append(job_table)
        for step in job["steps"]:
            step_table = {
                "number": step["number"],
                "job_id": job_table["id"],
                "name": step["name"],
                "status": step["status"],
                "conclusion": step["conclusion"],
                "started_at": step["started_at"],
                "completed_at": step["completed_at"]
            }
            steps_table.append(step_table)

    commit = {
        "id": run["head_commit"]["id"],
        "tree_id":run["head_commit"]["tree_id"],
        "message": run["head_commit"]["message"],
        "date_time": run["head_commit"]["timestamp"],
        "author_name": run["head_commit"]["author"]["name"],
        "author_email": run["head_commit"]["author"]["email"],
        "committer_name": run["head_commit"]["committer"]["name"],
        "committer_email": run["head_commit"]["committer"]["name"]
    }
    repositories = []
    repository = {
        "id": run["repository"]["id"],
        "name": run["repository"]["name"],
        "full_name": run["repository"]["full_name"],
        "description": run["repository"]["description"],
        "fork": run["repository"]["fork"],
        "owner_id": run["repository"]["owner"]["id"]
    }

    if run.get("head_repository", {"id": -1}) is None:
        head_repository = {
            "id": -1,
            "name": "None",
            "full_name": "None",
            "description": "None",
            "fork": "None",
            "owner_id": "None"
        }
    else:
        head_repository = {
            "id": run.get("head_repository", {"id": -1})["id"],
            "name": run.get("head_repository", {"name": "None"})["name"],
            "full_name": run.get("head_repository", {"full_name": "None"})["full_name"],
            "description": run.get("head_repository", {"description": "None"})["description"],
            "fork": run.get("head_repository", {"for": "None"})["fork"],
            "owner_id": run.get("head_repository", {"owner": {"id": -1}})["owner"]["id"]
        }

    repositories.append(repository)
    repositories.append(head_repository)

    actors = []
    owner = {
        "login": run["repository"]["owner"]["login"],
        "id": run["repository"]["owner"]["id"],
        "type": run["repository"]["owner"]["type"],
        "site_admin": run["repository"]["owner"]["site_admin"]
    }
    actors.append(owner)
    
    actor = {
        "login": run["actor"]["login"],
        "id": run["actor"]["id"],
        "type": run["actor"]["type"],
        "site_admin": run["actor"]["site_admin"]
    }
    actors.append(actor)

    triggering_actor = {
        "login": run.get("triggering_actor", {"login": "None"})["login"],
        "id": run.get("triggering_actor", {"id": -1})["id"],
        "type": run.get("triggering_actor", {"type": "None"})["type"],
        "site_admin": run.get("triggering_actor", {"site_admin": "None"})["site_admin"]
    }
    actors.append(triggering_actor)

    pull_requests = []
    for pr in run["pull_requests"]:
        table_pull_request = {
            "id": pr["id"],
            "number": pr["number"],
            "head_sha": pr["head"]["sha"],
            "head_repo_id": pr["head"]["repo"]["id"],
            "head_repo_url": pr["head"]["repo"]["url"],
            "base_sha": pr["base"]["sha"],
            "base_repo_id": pr["base"]["repo"]["id"],
            "base_repo_url": pr["base"]["repo"]["url"],
            "run_id": run["id"]
        }
        pull_requests.append(table_pull_request)

    return {
        "runs": [run_table], 
        "jobs": jobs_table, 
        "steps": steps_table, 
        "pull_requests": pull_requests, 
        "actors": actors,
        "repositories": repositories,
        "commits": [commit]
        }

def construct_repo_tables(repo, start_run = 0, end_run = -1):
    base_dir = "repositories_runs"
    repo_hash_name = repo.replace("/", "#####")
    with open("{0}/{1}.json".format(base_dir, repo_hash_name)) as runs_file:
        runs = json.load(runs_file)
    
    data = []
    runs_table = []
    jobs_table = []
    steps_table = []
    pull_requests_table = []
    actors_table = []
    repositories_table = []
    commits_table = []

    i = 0
    milestone = 20
    next_milestone = milestone
    next_token = 1
    for run in runs:
        print(i)
        i += 1
        if i == next_milestone:
            next_token = (next_token + 1) % len(tokens)
            next_milestone += milestone
        data = run_to_tables(run, tokens[next_token])
        if data is None:
            break
        runs_table.extend(data["runs"])
        jobs_table.extend(data["jobs"])
        steps_table.extend(data["steps"])
        pull_requests_table.extend(data["pull_requests"])
        actors_table.extend(data["actors"])
        repositories_table.extend(data["repositories"])
        commits_table.extend(data["commits"])

    Path("./tables/"+repo_hash_name).mkdir(parents=True, exist_ok=True)
    tables_dir = "./tables/" + repo_hash_name
    
    with open("{0}/{1}_data_tables.json".format(tables_dir, repo_hash_name), 'w') as dt:
        json.dump(data, dt)

    with open("{0}/{1}_runs_table.json".format(tables_dir, repo_hash_name), "w") as rt:
        json.dump(runs_table, rt)

    with open("{0}/{1}_jobs_table.json".format(tables_dir, repo_hash_name), "w") as jt:
        json.dump(jobs_table, jt)

    with open("{0}/{1}_steps_table.json".format(tables_dir, repo_hash_name), "w") as st:
        json.dump(steps_table, st)

    with open("{0}/{1}_pull_requests_table.json".format(tables_dir, repo_hash_name), "w") as pr:
        json.dump(pull_requests_table, pr)

    with open("{0}/{1}_actors_table.json".format(tables_dir, repo_hash_name), "w") as at:
        json.dump(actors_table, at)

    with open("{0}/{1}_repositories_table.json".format(tables_dir, repo_hash_name), "w") as rt:
        json.dump(repositories_table, rt)

    with open("{0}/{1}_commits_table.json".format(tables_dir, repo_hash_name), 'w') as ct:
        json.dump(commits_table, ct)


if __name__ == "main":
    list_repos = os.listdir("repositories_runs")
    list_repos = [repo[:-5] for repo in list_repos]

    next_token = 1
    for repo in list_repos[25:]:
        logging.info("constructing tables for repository: {}".format(repo))
        construct_repo_tables(repo)
