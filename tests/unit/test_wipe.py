# (C) Copyright 2011- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import pytest
from pyfdb.pyfdb import FDBException
from pathlib import Path

BASE_REQUEST = {
    "class": "rd",
    "expver": "xxxx",
    "stream": "oper",
    "type": "fc",
    "date": "20000101",
    "time": "0000",
    "domain": "g",
    "levtype": "pl",
    "levelist": "300",
    "param": "138",
    "step": "0",
}

def ls(path):
    return [p for p in Path(path).rglob("*")]

def populate_fdb(fdb):
    # Write 4 fields to the FDB based on BASE_REQUEST
    requests = [
        BASE_REQUEST.copy() for i in range(4)
    ]

    # Modify on each of the 3 levels of the schema
    requests[1]["step"] = "1"
    requests[2]["date"] = "20000102"
    requests[3]["levtype"] = "sfc"
    del requests[3]["levelist"]

    NFIELDS = 4

    bytes = b"-1 Kelvin"
    for i in range(NFIELDS):
        key = requests[i]
        fdb.archive(bytes, key=key)
    fdb.flush()

    assert len([x for x in fdb.list()]) == NFIELDS
    return NFIELDS

def test_wipe_simple(setup_fdb_tmp_dir):
    testdir, fdb = setup_fdb_tmp_dir()

    NFIELDS = populate_fdb(fdb)

    Npaths = len(ls(testdir))
    assert Npaths > 0

    # Wipe without doit: Do not actually delete anything.
    fdb.wipe({"class":"rd"})
    assert len([x for x in fdb.list()]) == NFIELDS

    # Wipe, do it
    fdb.wipe({"class":"rd"}, doit=True)
    assert len([x for x in fdb.list()]) == 0
    assert len(ls(testdir)) == 0

def test_wipe_polluted(setup_fdb_tmp_dir):
    testdir, fdb = setup_fdb_tmp_dir()

    NFIELDS = populate_fdb(fdb)

    # Add a junk file to each subdirectory
    for subdir in Path(testdir).rglob("*"):
        if subdir.is_dir():
            (subdir / "junk").touch()
            
    Npaths = len(ls(testdir))
    assert Npaths > 0

    # fdb does not allow you to unrecognised files by default
    with pytest.raises(FDBException):
        fdb.wipe({"class":"rd"}, doit=True)

    # Nothing deleted
    assert len(ls(testdir)) == Npaths 
    assert len([x for x in fdb.list()]) == NFIELDS

    fdb.wipe({"class":"rd"}, doit=True, unsafeWipeAll=True)
    
    # All files and directories deleted
    assert len([x for x in fdb.list()]) == 0
    assert len(ls(testdir)) == 0 

