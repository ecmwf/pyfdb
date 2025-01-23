
import pytest
from eccodes import StreamReader

import tests.util as util
from pyfdb.pyfdb import Key, Request

from tests.fixtures import setup_fdb_tmp_dir


def test_archive_key(setup_fdb_tmp_dir):
    tmp_root_dir, fdb = setup_fdb_tmp_dir()

    filename = util.get_test_data_root() / "x138-300.grib"

    key = Key(
        {
            "class": "rd",
            "date": "20191110",
            "domain": "g",
            "expver": "xxxx",
            "levelist": "300",
            "levtype": "pl",
            "param": "138",
            "step": "0",
            "stream": "oper",
            "time": "0000",
            "type": "an",
        }
    )

    fdb.archive(open(filename, "rb").read(), key)
    fdb.flush()


def test_archive_request(setup_fdb_tmp_dir):
    tmp_root_dir, fdb = setup_fdb_tmp_dir()

    filename = util.get_test_data_root() / "x138-300.grib"

    request = Request(
        {
            "class": "rd",
            "date": "20191110",
            "domain": "g",
            "expver": "xxxx",
            "levelist": "300",
            "levtype": "pl",
            "param": "138",
            "step": "0",
            "stream": "oper",
            "time": "0000",
            "type": "an",
        }
    )

    fdb.archive(open(filename, "rb").read(), request)
    fdb.flush()


def test_archive_dict(setup_fdb_tmp_dir):
    tmp_root_dir, fdb = setup_fdb_tmp_dir()

    filename = util.get_test_data_root() / "x138-300.grib"

    dict1 = {
        "class": "rd",
        "date": "20191110",
        "domain": "g",
        "expver": "xxxx",
        "levelist": "300",
        "levtype": "pl",
        "param": "138",
        "step": "0",
        "stream": "oper",
        "time": "0000",
        "type": "an",
    }

    fdb.archive(open(filename, "rb").read(), dict1)
    fdb.archive(open(filename, "rb").read(), Request(dict1))
    fdb.flush()


    # Retrieve the data saved with dict 1
    dataretriever1 = fdb.retrieve(dict1)
    reader = StreamReader(dataretriever1)
    grib = next(reader)
    grib_data_1 = grib.data

    list_iterator = fdb.list(dict1, duplicates=True, keys=True)
    assert len([x for x in list_iterator]) == 1

    with pytest.raises(StopIteration) as stop_iteration:   
        next(reader)

def test_archive_none(setup_fdb_tmp_dir):
    tmp_root_dir, fdb = setup_fdb_tmp_dir()

    filename = util.get_test_data_root() / "x138-300.grib"

    dict1 = {
        "class": "rd",
        "date": "20191110",
        "domain": "g",
        "expver": "xxxx",
        "levelist": "300",
        "levtype": "pl",
        "param": "138",
        "step": "0",
        "stream": "oper",
        "time": "0000",
        "type": "an",
    }

    fdb.archive(open(filename, "rb").read())
    fdb.flush()

    # Retrieve the data saved with dict 1
    dataretriever1 = fdb.retrieve(dict1)
    reader = StreamReader(dataretriever1)
    grib = next(reader)
    grib_data_1 = grib.data

    list_iterator = fdb.list(dict1, duplicates=True, keys=True)
    assert len([x for x in list_iterator]) == 1

    with pytest.raises(StopIteration) as stop_iteration:   
        next(reader)


