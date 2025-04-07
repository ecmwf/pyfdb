import pytest
from eccodes import StreamReader

import tests.util as util
from pyfdb.pyfdb import FDB, Key, Request

STATIC_DICTIONARY = {
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


def assert_one_field(fdb: FDB):
    dataretriever = fdb.retrieve(STATIC_DICTIONARY)
    reader = StreamReader(dataretriever)
    _ = next(reader)

    list_iterator = fdb.list(STATIC_DICTIONARY, duplicates=True, keys=True)
    assert len([x for x in list_iterator]) == 1

    with pytest.raises(StopIteration) as _:
        next(reader)


def test_archive_none(setup_fdb_tmp_dir):
    _, fdb = setup_fdb_tmp_dir()

    filename = util.get_test_data_root() / "x138-300.grib"

    fdb.archive(open(filename, "rb").read())
    fdb.flush()

    assert_one_field(fdb)


def test_archive_no_request(setup_fdb_tmp_dir):
    _, fdb = setup_fdb_tmp_dir()

    filename = util.get_test_data_root() / "x138-300.grib"

    fdb.archive(open(filename, "rb").read(), None)
    fdb.flush()

    assert_one_field(fdb)


def test_archive_no_request_no_key(setup_fdb_tmp_dir):
    """This results in the resolution of request data from the corresponding fdb call"""
    _, fdb = setup_fdb_tmp_dir()

    filename = util.get_test_data_root() / "x138-300.grib"

    fdb.archive(open(filename, "rb").read(), None, None)
    fdb.flush()

    assert_one_field(fdb)


def test_archive_key(setup_fdb_tmp_dir):
    _, fdb = setup_fdb_tmp_dir()

    filename = util.get_test_data_root() / "x138-300.grib"

    key = Key(STATIC_DICTIONARY)

    fdb.archive(open(filename, "rb").read(), key=key)
    fdb.flush()


def test_archive_request(setup_fdb_tmp_dir):
    _, fdb = setup_fdb_tmp_dir()

    filename = util.get_test_data_root() / "x138-300.grib"

    request = Request(STATIC_DICTIONARY)

    fdb.archive(open(filename, "rb").read(), request=request)
    fdb.flush()


def test_archive_request_dict(setup_fdb_tmp_dir):
    """Tests whether archival with dict and request leads to the same result, by checking the fdb list output"""
    _, fdb = setup_fdb_tmp_dir()

    filename = util.get_test_data_root() / "x138-300.grib"

    fdb.archive(open(filename, "rb").read(), request=STATIC_DICTIONARY)
    fdb.archive(open(filename, "rb").read(), request=Request(STATIC_DICTIONARY))
    fdb.flush()

    assert_one_field(fdb)


def test_archive_key_dict(setup_fdb_tmp_dir):
    """Tests whether archival with dict and key leads to the same result, by checking the fdb list output"""
    _, fdb = setup_fdb_tmp_dir()

    filename = util.get_test_data_root() / "x138-300.grib"

    fdb.archive(open(filename, "rb").read(), key=STATIC_DICTIONARY)
    fdb.archive(open(filename, "rb").read(), key=Key(STATIC_DICTIONARY))
    fdb.flush()

    assert_one_field(fdb)


def test_archive_request_dict_key(setup_fdb_tmp_dir):
    """Tests whether archival with request dict and key leads to the same result, by checking the fdb list output"""
    _, fdb = setup_fdb_tmp_dir()

    filename = util.get_test_data_root() / "x138-300.grib"

    fdb.archive(open(filename, "rb").read(), request=STATIC_DICTIONARY)
    fdb.archive(open(filename, "rb").read(), key=Key(STATIC_DICTIONARY))
    fdb.flush()

    assert_one_field(fdb)


def test_archive_request_key_dict(setup_fdb_tmp_dir):
    """Tests whether archival with request and key dict leads to the same result, by checking the fdb list output"""
    _, fdb = setup_fdb_tmp_dir()

    filename = util.get_test_data_root() / "x138-300.grib"

    fdb.archive(open(filename, "rb").read(), key=STATIC_DICTIONARY)
    fdb.archive(open(filename, "rb").read(), request=Request(STATIC_DICTIONARY))
    fdb.flush()

    assert_one_field(fdb)


def test_archive_request_dict_key_dict(setup_fdb_tmp_dir):
    """Tests whether archival with request and key dict leads to the same result, by checking the fdb list output"""
    _, fdb = setup_fdb_tmp_dir()

    filename = util.get_test_data_root() / "x138-300.grib"

    fdb.archive(open(filename, "rb").read(), key=STATIC_DICTIONARY)
    fdb.archive(open(filename, "rb").read(), request=STATIC_DICTIONARY)
    fdb.flush()

    assert_one_field(fdb)
