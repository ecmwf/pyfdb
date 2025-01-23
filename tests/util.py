import git
import os

from pathlib import Path


def get_test_data_root() -> Path:
    return find_git_root() /  "tests" / "data" 


def find_git_root() -> Path:
    git_repo = git.Repo(os.getcwd(), search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")

    return Path(git_root)
