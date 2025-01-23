#!/usr/bin/env python

# (C) Copyright 1996- ECMWF.

# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import os
import shutil
import util

from eccodes import StreamReader

import pyfdb

fdb = pyfdb.FDB()

# Archive
key = {
    "domain": "g",
    "stream": "oper",
    "levtype": "pl",
    "levelist": "300",
    "date": "20191110",
    "time": "0000",
    "step": "0",
    "param": "138",
    "class": "rd",
    "type": "an",
    "expver": "xxxx",
}

def test_archival_read():

    filename = util.find_git_root() / "tests" /  "x138-300.grib"
    pyfdb.archive(open(filename, "rb").read())

    key["levelist"] = "400"
    filename = os.path.join(os.path.dirname(__file__), "x138-400.grib")
    pyfdb.archive(open(filename, "rb").read())

    key["expver"] = "xxxy"
    filename = os.path.join(os.path.dirname(__file__), "y138-400.grib")
    pyfdb.archive(open(filename, "rb").read())
    pyfdb.flush()

    # List
    request = {
        "class": "rd",
        "expver": "xxxx",
        "stream": "oper",
        "date": "20191110",
        "time": "0000",
        "domain": "g",
        "type": "an",
        "levtype": "pl",
        "step": 0,
        "levelist": [300, "500"],
        "param": ["138", 155, "t"],
    }
    print("direct function, request as dictionary:", request)
    for el in pyfdb.list(request, True):
        assert el["path"]
        assert el["path"].find("rd:xxxx:oper:20191110:0000:g/an:pl.") != -1
        assert "keys" not in el

