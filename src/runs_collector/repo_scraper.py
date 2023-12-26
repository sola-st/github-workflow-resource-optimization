import requests

def scrape_repos(query, headers=None):
    template = "https://api.github.com/search/repositories?q={0}"
    req = template.format(query)
    response = requests.get(req, headers=headers)

    projects_list = response.json()["items"]

    return projects_list

def parse_repo(repo_meta):
    pass