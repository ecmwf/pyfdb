import os
import random
from typing import Callable, Tuple

import pytest

import pyfdb
from pyfdb.pyfdb import FDB
import tests.util as util


@pytest.fixture
def setup_fdb_tmp_dir(tmp_path_factory: pytest.TempPathFactory) -> Callable[[], Tuple[str, FDB]]:

    def create_randomized_fdb_path():
        tests_dir = util.get_test_data_root()
        tmp_dir = tmp_path_factory.mktemp(os.path.normpath(str(random.randint(a=0, b=10000))), numbered=True)

        config = dict(
            type="local",
            engine="toc",
            schema=str(tests_dir / "default_fdb_schema"),
            spaces=[
                dict(
                    handler="Default",
                    roots=[
                        {"path": str(tmp_dir)},
                    ],
                )
            ],
        )

        return str(tmp_dir), pyfdb.FDB(config)
    
    # Return a function to randomize on each call
    return create_randomized_fdb_path

