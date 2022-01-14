#!/usr/bin/env python

# (C) Copyright 1996- ECMWF.

# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.

import pyfdb
import shutil

fdb = pyfdb.FDB()

### Archive ###
key = {
    'domain': 'g',
    'stream': 'oper',
    'levtype': 'pl',
    'levelist': '300',
    'date': '20191110',
    'time': '0000',
    'step': '0',
    'param': '138',
    'class': 'rd',
    'type': 'an',
    'expver': 'xxxx'
}

filename = 'x138-300.grib'
fdb.archive(open(filename, "rb").read(), key)

key['levelist'] = '400'
filename = 'x138-400.grib'
pyfdb.archive(open(filename, "rb").read())

key['expver'] = 'xxxy'
filename = 'y138-400.grib'
fdb.archive(open(filename, "rb").read())
fdb.flush()

### List ###
request = {
    'class': 'rd',
    'expver': 'xxxx',
    'stream': 'oper',
    'date': '20191110',
    'time': '0000',
    'domain': 'g',
    'type': 'an',
    'levtype': 'pl',
    'step': 0,
    'levelist': [300, '500'],
    'param': ['138', 155, 't']
}
print('direct function, request as dictionary:', request)
for el in pyfdb.list(request):
    print(el)

request['levelist'] = ['100', '200', '300', '400', '500', '700', '850', '1000']
request['param'] = '138'
print('')
print('direct function, updated dictionary:', request)
for el in pyfdb.list(request):
    print(el)


# as an alternative, create a FDB instance and start queries from there
request['levelist'] = ['400', '500', '700', '850', '1000']
print('')
print('fdb object, request as dictionary:', request)
for el in fdb.list(request):
    print(el)
#
# print('')
# print('list ALL:')
# for el in fdb.list():
#     print(el)



### Retrieve ###
request = {
    'domain': 'g',
    'stream': 'oper',
    'levtype': 'pl',
    'step': '0',
    'expver': 'xxxx',
    'date': '20191110',
    'class': 'rd',
    'levelist': '300',
    'param': '138',
    'time': '0000',
    'type': 'an'
}

filename = 'x138-300bis.grib'
print('')
print('save to file ', filename)
with open(filename, 'wb') as o, fdb.retrieve(request) as i:
    shutil.copyfileobj(i, o)

request['levelist'] = '400'
filename = 'x138-400bis.grib'
print('save to file ', filename)
with open(filename, 'wb') as o, fdb.retrieve(request) as i:
    shutil.copyfileobj(i, o)

request['expver'] = 'xxxy'
filename = 'y138-400bis.grib'
print('save to file ', filename)
with open(filename, 'wb') as o, pyfdb.retrieve(request) as i:
    shutil.copyfileobj(i, o)



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
print('')
print('FDB retrieve')
print('direct function, retrieve from request:', request)
datareader = pyfdb.retrieve(request)

print('')
print('reading a small chunk')
chunk = datareader.read(10)
print(chunk)
print('tell()', datareader.tell())

print('go back (partially) - seek(2)')
datareader.seek(2)
print('tell()', datareader.tell())

print('reading a larger chunk')
chunk = datareader.read(40)
print(chunk)

print('go back - seek(0)')
datareader.seek(0)

print('')
print('decode GRIB')
from pyeccodes import Reader
reader = Reader(datareader)
grib = next(reader)
grib.dump()

request['levelist'] = [300, '400']
request['expver'] = 'xxxx'
filename = 'foo.grib'

print('')
print('save to file ', filename)
with open(filename, 'wb') as o, fdb.retrieve(request) as i:
    shutil.copyfileobj(i, o)

