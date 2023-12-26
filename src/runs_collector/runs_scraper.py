import requests
import json
import pandas as pd
import time


with open("tokens.json") as tk:
    tokens = json.load(tk)
    
def get_repo_runs_list(repo, tokens):
    token_i = 0
    headers = {
        #'Accept': 'application/vnd.github+json',
        'Authorization': tokens[token_i]
        #'X-GitHub-Api-Version': '2022-11-28',
    }
    page = 1
    per_page = 100
    query_template = "https://api.github.com/repos/{0}/actions/runs?page={1}&per_page={2}"
    req = requests.get(query_template.format(repo, page, per_page), headers=headers)
    first_page = req.json()
    total_runs_number = first_page["total_count"]
    pages_total = int(total_runs_number/per_page) + 1
    runs_per_page = []
    milestone_size = 20
    next_milestone = milestone_size
    print("Total number of runs for repo {} : {}".format(repo, total_runs_number))
    print("Number of pages to scrape:", pages_total)
    for p in range(1, pages_total+1, 1):
        #time.sleep(2)
        print("scraping page:", p)
        if p == next_milestone:
            token_i = (token_i + 1) % len(tokens)
            headers = {
                    #'Accept': 'application/vnd.github+json',
                    'Authorization': tokens[token_i]
                    #'X-GitHub-Api-Version': '2022-11-28',
                }
            next_milestone += milestone_size

        success = False
        failures = 0
        while not success:
            try:
                req = requests.get(query_template.format(repo, p, per_page), headers=headers)
                runs = req.json()
                runs_per_page.extend(runs["workflow_runs"])
                success = True
            except Exception as e:
                print("Error:", e)
                failures += 1
                if failures > 30:
                    break
                time.sleep(1)
                
    base_dir = "repositories_runs"
    with open("{0}/{1}.json".format(base_dir, repo.replace("/", "#####")), 'w') as rj:
        json.dump(runs_per_page, rj)
    return runs_per_page

def get_jobs(url, token):
    headers = {
        #'Accept': 'application/vnd.github+json',
        'Authorization': token
        #'X-GitHub-Api-Version': '2022-11-28',
    }
    req = requests.get(url, headers=headers)
    return req.json()

def scrape_runs(repos_list, exit=False):
    i = 0
    for repo in repos_list:
        i += 1 
        #try:
        print(i, "getting runs for repo:", repo)
        get_repo_runs_list(repo, tokens)
        #except Exception as e:
        #    print(e)
        #    if exit:
        #        return
        print()
def check_total_runs(full_name, headers=None):
    template = "https://api.github.com/repos/{0}/actions/runs"
    req = template.format(full_name)
    response = requests.get(req, headers=headers)
    try:
        total_count = response.json()["total_count"]
        return total_count
    except Exception as e:
        print(e)
        return -1

if __name__ == "main":
    df = pd.read_csv("repositories.csv")
    df_sample = df.sample(frac=0.001)
    repos_list = df_sample["repository"].to_list()


    #get_repo_runs_list("sola-st/DynaPyt", tokens)

    #scrape_runs(repos_list)