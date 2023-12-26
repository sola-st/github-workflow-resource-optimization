
class ModifiedFile():
    def __init__(
                    self, 
                    commit_hash, 
                    repo, 
                    author,
                    old_path, 
                    new_path, 
                    file_name, 
                    change_type, 
                    diff_parsed,
                    source_code, 
                    source_code_before, 
                    c_date, 
                    c_timezone,
                    branches,
                    in_main_branch,
                    merge
                ):

        self.commit_hash = commit_hash
        self.repo = repo
        self.author = author
        self.old_path = old_path
        self.new_path = new_path
        self.file_name = file_name
        self.change_type = change_type
        self.diff_parsed = diff_parsed
        self.source_code = source_code
        self.source_code_before = source_code_before
        self.c_date = c_date
        self.c_timezone = c_timezone
        self.branches = branches
        self.in_main_branch = in_main_branch
        self.merge = merge

    def export_as_dict(self):
        return {
            "commit_hash": self.commit_hash,
            "repo": self.repo,
            "author": self.author,
            "old_path": self.old_path,
            "new_path": self.new_path,
            "file_name": self.file_name,
            "change_type": self.change_type,
            "diff_parsed": self.diff_parsed,
            "source_code": self.source_code,
            "source_code_before": self.source_code_before,
            "c_date": self.c_date,
            "c_timezone": self.c_timezone,
            "branches": self.branches,
            "in_main_branch": self.in_main_branch,
            "merge": self.merge
        }
