#!/usr/bin/env python

# (C) Copyright 1996- ECMWF.

# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
import os

from pyeccodes import Reader

import pyfdb


def test_archive():
    """
    Test the archive function
    """

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

    def archive_grib(grib: str):
        filename = os.path.join(os.path.dirname(__file__), grib)
        pyfdb.archive(open(filename, "rb").read())

    archive_grib("x138-300.grib")

    key["levelist"] = "400"
    archive_grib("x138-400.grib")

    key["expver"] = "xxxy"
    archive_grib("y138-400.grib")

    pyfdb.flush()


def test_list():
    """
    Test the list function
    """

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

    request["levelist"] = ["100", "200", "300", "400", "500", "700", "850", "1000"]
    request["param"] = "138"
    print("")
    print("direct function, updated dictionary:", request)
    it = iter(pyfdb.list(request, True, True))

    el = next(it)
    assert el["path"]
    assert el["path"].find("rd:xxxx:oper:20191110:0000:g/an:pl.") != -1
    assert el["keys"]
    keys = el["keys"]
    assert keys["class"] == "rd"
    assert keys["levelist"] == "300"

    el = next(it)
    assert el["path"]
    assert el["path"].find("rd:xxxx:oper:20191110:0000:g/an:pl.") != -1
    assert el["keys"]
    keys = el["keys"]
    assert keys["class"] == "rd"
    assert keys["levelist"] == "400"

    try:
        el = next(it)
        assert False, "returned unexpected field"
    except StopIteration:
        assert True, "field listing completed"

    # as an alternative, create a FDB instance and start queries from there
    request["levelist"] = ["400", "500", "700", "850", "1000"]
    print("")
    print("fdb object, request as dictionary:", request)

    fdb = pyfdb.FDB()
    for el in fdb.list(request, True, True):
        assert el["path"]
        assert el["path"].find("rd:xxxx:oper:20191110:0000:g/an:pl.") != -1
        assert el["keys"]
        keys = el["keys"]
        assert keys["class"] == "rd"
        assert keys["levelist"] == "400"


def test_list_depth():
    """
    Test fdb list depth option
    """

    request = {
        "class": "rd",
        "expver": ["xxxx", "xxxy"],
        "stream": "oper",
        "date": "20191110",
        "time": "0000",
        "domain": "g",
        "type": "an",
        "levtype": "pl",
        "step": 0,
        "levelist": ["300", "400"],
        "param": "138",
    }

    print("test list: request={0}".format(request))

    print("list: depth=1")

    list = [
        {"class": "rd", "date": "20191110", "domain": "g", "expver": "xxxx", "stream": "oper", "time": "0000"},
        {"class": "rd", "date": "20191110", "domain": "g", "expver": "xxxy", "stream": "oper", "time": "0000"},
    ]

    for id, el in enumerate(pyfdb.list(request, True, True, False, 1)):
        assert "keys" in el
        assert el["keys"] == list[id]
        # print("%(keys)s" % el)

    print("list: depth=2")

    list = [
        {
            "class": "rd",
            "date": "20191110",
            "domain": "g",
            "expver": "xxxx",
            "stream": "oper",
            "time": "0000",
            "levtype": "pl",
            "type": "an",
        },
        {
            "class": "rd",
            "date": "20191110",
            "domain": "g",
            "expver": "xxxy",
            "stream": "oper",
            "time": "0000",
            "levtype": "pl",
            "type": "an",
        },
    ]

    for id, el in enumerate(pyfdb.list(request, True, True, False, 2)):
        assert "keys" in el
        assert el["keys"] == list[id]
        # print("%(keys)s" % el)

    print("list: depth=3")

    list = [
        {
            "class": "rd",
            "date": "20191110",
            "domain": "g",
            "expver": "xxxx",
            "stream": "oper",
            "time": "0000",
            "levtype": "pl",
            "type": "an",
            "levelist": "300",
            "param": "138",
            "step": "0",
        },
        {
            "class": "rd",
            "date": "20191110",
            "domain": "g",
            "expver": "xxxx",
            "stream": "oper",
            "time": "0000",
            "levtype": "pl",
            "type": "an",
            "levelist": "400",
            "param": "138",
            "step": "0",
        },
        {
            "class": "rd",
            "date": "20191110",
            "domain": "g",
            "expver": "xxxy",
            "stream": "oper",
            "time": "0000",
            "levtype": "pl",
            "type": "an",
            "levelist": "400",
            "param": "138",
            "step": "0",
        },
    ]

    for id, el in enumerate(pyfdb.list(request, True, True, False, 3)):
        assert "keys" in el
        assert el["keys"] == list[id]
        # print("%(keys)s" % el)

    # default depth is 3
    for id, el in enumerate(pyfdb.list(request, True, True, False)):
        assert "keys" in el
        assert el["keys"] == list[id]
        # print("%(keys)s" % el)


def test_retrieve():
    """
    Test the retrieve function
    """
    import shutil

    fdb = pyfdb.FDB()

    def retrieve_grib(grib: str):
        filename = os.path.join(os.path.dirname(__file__), grib)
        with open(filename, "wb") as dest, fdb.retrieve(request) as src:
            shutil.copyfileobj(src, dest)

    request = {
        "domain": "g",
        "stream": "oper",
        "levtype": "pl",
        "step": "0",
        "expver": "xxxx",
        "date": "20191110",
        "class": "rd",
        "levelist": "300",
        "param": "138",
        "time": "0000",
        "type": "an",
    }

    retrieve_grib("x138-300bis.grib")

    request["levelist"] = "400"
    retrieve_grib("x138-400bis.grib")

    request["expver"] = "xxxy"
    retrieve_grib("y138-400bis.grib")

    # request = {
    #     'class': 'od',
    #     'expver': '0001',
    #     'stream': 'oper',
    #     'date': '20040118',
    #     'time': '0000',
    #     'domain': 'g',
    #     'type': 'an',
    #     'levtype': 'sfc',
    #     'step': 0,
    #     'param': 151
    # }
    print("")
    print("FDB retrieve")
    print("direct function, retrieve from request:", request)
    datareader = pyfdb.retrieve(request)

    print("")
    print("reading a small chunk")
    chunk = datareader.read(10)
    print(chunk)
    print("tell()", datareader.tell())

    print("go back (partially) - seek(2)")
    datareader.seek(2)
    print("tell()", datareader.tell())

    print("reading a larger chunk")
    chunk = datareader.read(40)
    print(chunk)

    print("go back - seek(0)")
    datareader.seek(0)

    print("")
    print("decode GRIB")

    reader = Reader(datareader)
    grib = next(reader)
    grib.dump()

    request["levelist"] = [300, "400"]
    request["expver"] = "xxxx"
    filename = os.path.join(os.path.dirname(__file__), "foo.grib")

    print("")
    print("save to file ", filename)
    with open(filename, "wb") as dest, fdb.retrieve(request) as src:
        shutil.copyfileobj(src, dest)
