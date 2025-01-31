import os
from pathlib import Path

import git
import numpy as np
import pytest
from eccodes import StreamReader

from pyfdb.pyfdb import FDB, Request


def get_test_data_root() -> Path:
    return Path(find_git_root()) / "tests" / "data"


def find_git_root() -> Path:
    git_repo = git.Repo(os.getcwd(), search_parent_directories=True)
    git_root = git_repo.git.rev_parse("--show-toplevel")

    return Path(git_root)


def check_grib_files_for_same_content(file1: Path, file2: Path):
    with file1.open("rb") as file1:
        with file2.open("rb") as file2:

            reader1 = StreamReader(file1)
            grib1 = next(reader1)

            reader2 = StreamReader(file2)
            grib2 = next(reader2)

            return check_numpy_array_equal(grib1.data, grib2.data)


def check_numpy_array_equal(array1, array2):
    return np.array_equal(array1, array2)


def assert_one_field(fdb: FDB, request: dict[str, str] | Request):
    dataretriever = fdb.retrieve(request)
    reader = StreamReader(dataretriever)
    _ = next(reader)

    list_iterator = fdb.list(request, duplicates=True, keys=True)
    assert len([x for x in list_iterator]) == 1

    with pytest.raises(StopIteration) as _:
        next(reader)
