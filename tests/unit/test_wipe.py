from pyfdb.pyfdb import FDB, Request
from tests import util

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


def test_wipe(setup_fdb_tmp_dir):
    _, fdb = setup_fdb_tmp_dir()

    filename = util.get_test_data_root() / "x138-300.grib"

    fdb: FDB = fdb

    fdb.archive(open(filename, "rb").read())
    fdb.flush()

    wipe_iterator = fdb.wipe(Request(STATIC_DICTIONARY), FDB.WipeConfig(doit=True))

    next(wipe_iterator)
