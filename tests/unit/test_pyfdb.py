# Copyright 2021 European Centre for Medium-Range Weather Forecasts (ECMWF)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import os
import shutil

import pytest
from conftest import ValueStorage
from packaging import version
from pyeccodes import Reader

import pyfdb


def test_version():
    try:
        ValueStorage.version = version.Version(pyfdb.__version__)
    except Exception:
        pytest.fail("Failed parsing pyfdb version.")


def test_init():
    ValueStorage.fdb = pyfdb.FDB()


def test_archive():
    dir = os.path.dirname(os.path.realpath(__file__))
    fdb = ValueStorage.fdb

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

    filename = os.path.join(dir, "x138-300.grib")
    fdb.archive(open(filename, "rb").read(), key)

    key["levelist"] = "400"
    filename = os.path.join(dir, "x138-400.grib")
    fdb.archive(open(filename, "rb").read())

    key["expver"] = "xxxy"
    filename = os.path.join(dir, "y138-400.grib")
    fdb.archive(open(filename, "rb").read())
    fdb.flush()


def test_list():
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

    n = 0
    res = ""
    for el in pyfdb.list(request):
        res += str(el)
        n += 1
    assert n == 1
    expect = (
        "{class=rd,expver=xxxx,stream=oper,date=20191110,time=0000,domain=g}"
        + "{type=an,levtype=pl}{step=0,levelist=300,param=138}"
    )
    assert res == expect

    request["levelist"] = ["100", "200", "300", "400", "500", "700", "850", "1000"]
    request["param"] = "138"
    n = 0
    res = ""
    for el in pyfdb.list(request):
        res += str(el)
        n += 1
    assert n == 2
    expect = (
        "{class=rd,expver=xxxx,stream=oper,date=20191110,time=0000,domain=g}{type=an,levtype=pl}"
        + "{step=0,levelist=300,param=138}{class=rd,expver=xxxx,stream=oper,date=20191110,time=0000,domain=g}"
        + "{type=an,levtype=pl}{step=0,levelist=400,param=138}"
    )
    assert res == expect

    request["levelist"] = ["400", "500", "700", "850", "1000"]
    res = ""
    n = 0
    for el in ValueStorage.fdb.list(request):
        res += str(el)
        n += 1
    assert n == 1
    expect = (
        "{class=rd,expver=xxxx,stream=oper,date=20191110,time=0000,domain=g}"
        + "{type=an,levtype=pl}{step=0,levelist=400,param=138}"
    )
    assert res == expect

    res = ""
    n = 0
    for el in ValueStorage.fdb.list():
        res += str(el)
        n += 1
    assert n == 3
    expect = (
        "{class=rd,expver=xxxx,stream=oper,date=20191110,time=0000,domain=g}"
        + "{type=an,levtype=pl}{step=0,levelist=300,param=138}"
        + "{class=rd,expver=xxxx,stream=oper,date=20191110,time=0000,domain=g}"
        + "{type=an,levtype=pl}{step=0,levelist=400,param=138}"
        + "{class=rd,expver=xxxy,stream=oper,date=20191110,time=0000,domain=g}"
        + "{type=an,levtype=pl}{step=0,levelist=400,param=138}"
    )
    assert res == expect


def test_retrieve():
    dir = ValueStorage.tmp_path

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

    filename = os.path.join(dir, "x138-300bis.grib")
    with open(filename, "wb") as o, ValueStorage.fdb.retrieve(request) as i:
        shutil.copyfileobj(i, o)

    request["levelist"] = "400"
    filename = os.path.join(dir, "x138-400bis.grib")
    with open(filename, "wb") as o, ValueStorage.fdb.retrieve(request) as i:
        shutil.copyfileobj(i, o)

    request["expver"] = "xxxy"
    filename = os.path.join(dir, "y138-400bis.grib")
    with open(filename, "wb") as o, pyfdb.retrieve(request) as i:
        shutil.copyfileobj(i, o)

    datareader = pyfdb.retrieve(request)

    chunk = datareader.read(10)
    assert chunk == bytearray(b"GRIB2\x0e\x0e\x01\x00\x00")

    assert datareader.tell() == 10

    datareader.seek(2)
    assert datareader.tell() == 2

    chunk = datareader.read(40)
    assert chunk == bytearray(
        b"IB2\x0e\x0e\x01\x00\x004\x80b\x96\xff\x80\x8ad\x01\x90\x13\x0b\n\x00\x00\x01\x00\x00\x00\x00\x00\x00\x15\x00\x00\x00\x00\x00\x00\x00\x00\x00"  # noqa
    )

    datareader.seek(0)

    reader = Reader(datareader)
    grib = next(reader)
    grib.dump()
