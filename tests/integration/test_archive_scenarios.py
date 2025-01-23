import numpy as np
from eccodes import StreamReader
import pytest

import tests.util as util
from pyfdb.pyfdb import FDBException, Key, Request

from tests.fixtures import setup_fdb_tmp_dir


def test_archive_different_keys(setup_fdb_tmp_dir):
    """Testing whether different keys lead to two entries in the FDB """
    _, fdb = setup_fdb_tmp_dir()

    filename = util.find_git_root() / "tests" / "data" / "x138-300.grib"

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

    fdb.archive(open(filename, "rb").read(), Key(dict1))

    dict2 = {
        "class": "od",
        "date": "20201110",
        "domain": "g",
        "expver": "xxxy",
        "levelist": "300",
        "levtype": "pl",
        "param": "139",
        "step": "0",
        "stream": "oper",
        "time": "0600",
        "type": "an",
    }

    fdb.archive(open(filename, "rb").read(), Key(dict2))
    fdb.flush()

    # Retrieve the data saved with key 1
    dataretriever1 = fdb.retrieve(dict1)
    reader = StreamReader(dataretriever1)
    grib = next(reader)
    grib_data_1 = grib.data

    # Retrieve the data saved with key 2
    dataretriever2 = fdb.retrieve(dict2)
    reader = StreamReader(dataretriever2)
    grib = next(reader)
    grib_data_2 = grib.data

    # Check whether the returned numpy arrays are equal
    assert np.array_equal(grib_data_1, grib_data_2)
    # Check for different memory locations
    assert grib_data_1.ctypes.data != grib_data_2.ctypes.data


def test_archive_different_request_key(setup_fdb_tmp_dir):
    """Test whether archival with key and correct request object lead to two identical entries """
    _, fdb = setup_fdb_tmp_dir()

    filename = util.find_git_root() / "tests" / "data" / "x138-300.grib"

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

    fdb.archive(open(filename, "rb").read(), Request(dict1))

    dict2 = {
        "class": "od",
        "date": "20201110",
        "domain": "g",
        "expver": "xxxy",
        "levelist": "300",
        "levtype": "pl",
        "param": "139",
        "step": "0",
        "stream": "oper",
        "time": "0600",
        "type": "an",
    }

    fdb.archive(open(filename, "rb").read(), Key(dict2))
    fdb.flush()

    # Retrieve the data saved with key 1
    dataretriever1 = fdb.retrieve(dict1)
    reader = StreamReader(dataretriever1)
    grib = next(reader)
    grib_data_1 = grib.data

    # Retrieve the data saved with key 2
    dataretriever2 = fdb.retrieve(dict2)
    reader = StreamReader(dataretriever2)
    grib = next(reader)
    grib_data_2 = grib.data

    # Check whether the returned numpy arrays are equal
    assert np.array_equal(grib_data_1, grib_data_2)
    # Check for different memory locations
    assert grib_data_1.ctypes.data != grib_data_2.ctypes.data


def test_archive_none_retrieve_wrong_key(setup_fdb_tmp_dir):
    """Test whether a archival of the grib message only lead to the correct behavior when looking
    it up with the derived request """
    tmp_root_dir, fdb = setup_fdb_tmp_dir()

    filename = util.get_test_data_root() / "x138-300.grib"

    fdb.archive(open(filename, "rb").read())
    fdb.flush()

    # Retrieve the data saved with dict 1
    dict1 = {
        "class": "rd",
        "date": "20191110",
        "domain": "g",
        "expver": "xxxy",  # THIS HAS CHANGED FROM xxxx
        "levelist": "300",
        "levtype": "pl",
        "param": "138",
        "step": "0",
        "stream": "oper",
        "time": "0000",
        "type": "an",
    }
    dataretriever1 = fdb.retrieve(dict1)
    reader = StreamReader(dataretriever1)

    # Expect it to fail with the wrong request...
    with pytest.raises(StopIteration) as stop_iteration:
        next(reader)

    # But succeed with the right one
    dict_correct = {
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

    # Retrieve the data saved with dict 1
    dataretriever_correct = fdb.retrieve(dict_correct)
    reader = StreamReader(dataretriever_correct)
    next(reader)


def test_archive_wrong_request(setup_fdb_tmp_dir):
    """Test the archival with a wrongly matched request. Tests for occurring FDBException """
    tmp_root_dir, fdb = setup_fdb_tmp_dir()

    filename = util.get_test_data_root() / "x138-300.grib"

    dict1 = {
        "class": "rd",
        "date": "20191110",
        "domain": "g",
        "expver": "xxxy",  # THIS HAS CHANGED FROM xxxx
        "levelist": "300",
        "levtype": "pl",
        "param": "138",
        "step": "0",
        "stream": "oper",
        "time": "0000",
        "type": "an",
    }

    with pytest.raises(FDBException) as fdb_exception:
        fdb.archive(open(filename, "rb").read(), Request(dict1))
        fdb.flush()


def test_archive_wrong_dict_as_request(setup_fdb_tmp_dir):
    """Test the archival with a wrongly matched dict. Tests for occurring FDBException """
    tmp_root_dir, fdb = setup_fdb_tmp_dir()

    filename = util.get_test_data_root() / "x138-300.grib"

    dict1 = {
        "class": "rd",
        "date": "20191110",
        "domain": "g",
        "expver": "xxxy",  # THIS HAS CHANGED FROM xxxx
        "levelist": "300",
        "levtype": "pl",
        "param": "138",
        "step": "0",
        "stream": "oper",
        "time": "0000",
        "type": "an",
    }

    with pytest.raises(FDBException) as fdb_exception:
        fdb.archive(open(filename, "rb").read(), dict1)
        fdb.flush()

