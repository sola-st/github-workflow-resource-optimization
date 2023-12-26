from pydriller import Repository
from .commit_modified_files import ModifiedFile
import os
import json

def collect_commits(commits_list, repo):
    i = 0
    modified_files = {
                        "Added": [],
                        "Deleted": [],
                        "Modified": [],
                        "Renamed": []
                    }
    commits_messages = []
    errors = []
    for commit in commits_list:
        #print(i, end="\r")
        try:
            i += 1
            author = commit.author.name
            sha = commit.hash
            c_date = commit.committer_date.replace().timestamp()
            if c_date < 1598918400:
                continue
            c_timezone = None
            branches = None
            in_main_branch = commit.in_main_branch
            merge = commit.merge
            msg = commit.msg
            commits_messages.append((repo, c_date, msg))
            for m in commit.modified_files:
                if str(m.change_type) == "ModificationType.ADD":
                    if ".github/workflows" in m.new_path or ".github/actions" in m.new_path:
                        modified_files["Added"].append(ModifiedFile(
                            sha, 
                            repo, 
                            author,
                            m.old_path,
                            m.new_path,
                            m.filename,
                            str(m.change_type),
                            m.diff_parsed,
                            m.content.decode(),
                            None,
                            c_date,
                            c_timezone,
                            branches,
                            in_main_branch,
                            merge
                        ))
                if str(m.change_type) == "ModificationType.DELETE":
                    if ".github/workflows" in m.old_path or ".github/actions" in m.old_path:
                        modified_files["Deleted"].append(ModifiedFile(
                            sha, 
                            repo, 
                            author,
                            m.old_path,
                            m.new_path,
                            m.filename,
                            str(m.change_type),
                            m.diff_parsed,
                            None,
                            m.content_before.decode(),
                            c_date,
                            c_timezone,
                            branches,
                            in_main_branch,
                            merge
                        ))
                if str(m.change_type) == "ModificationType.MODIFY":
                    if ".github/workflows" in m.old_path or ".github/actions" in m.old_path:
                        modified_files["Modified"].append(ModifiedFile(
                            sha, 
                            repo, 
                            author,
                            m.old_path,
                            m.new_path,
                            m.filename,
                            str(m.change_type),
                            m.diff_parsed,
                            m.content.decode(),
                            m.content_before.decode(),
                            c_date,
                            c_timezone,
                            branches,
                            in_main_branch,
                            merge
                        )) 
        except Exception as e:
            errors.append((repo, e))
            
    return modified_files, commits_messages, errors


def collect_commits_messages(commits_list, repo):
    commits_messages = []
    for commit in commits_list:
        #print(i, end="\r")
        c_date = commit.committer_date.replace().timestamp()
        if c_date < 1598918400:
            continue
        branches = commit.branches
        msg = commit.msg
        commits_messages.append((repo, branches, c_date, msg))

    return commits_messages

if __name__ == "__main__":
    collected_commits = []
    collected_messages = []
    errors = []
    for repo in repos_list.full_name:
        if not os.path.exists(os.path.join("clone_repos", repo)):
            Repo.clone_from("https://github.com/" + repo, os.path.join("clone_repos", repo))
        c_commits, commits_message, commits_errors = collect_commits(list(Repository(os.path.join("clone_repos", repo)).traverse_commits()), repo)
        collected_commits.append(c_commits)
        collected_messages.append(collected_messages)
        errors.append(commits_errors)
        with open("collected_commits_save_poins.json", "w") as ccsp:
            json.dump(
                [
                    {
                        "Added": [d.export_as_dict() for d in c["Added"]],
                        "Deleted": [d.export_as_dict() for d in c["Deleted"]],
                        "Modified": [d.export_as_dict() for d in c["Modified"]]
                    } for c in collected_commits
                ], 
                ccsp)