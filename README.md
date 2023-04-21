# pyfdb

This repository contains the source code and documentation of pyfdb, a python FDB client which interfaces the FDB5 library in the system.

:warning: This project is BETA and will be experimental for the foreseeable future. Interfaces and functionality are likely to change.

&nbsp;
## 1. Installation

Install pyfdb with python3 (>= 3.6) and pip as follows:
```bash
python3 -m pip install --upgrade git+https://github.com/ecmwf/pyfdb.git@master
```

Or from PyPi (not yet available):
```bash
python3 -m pip install pyfdb
```

Or with the Conda package manager:
```bash
conda create -n client python=3.7
conda activate client
python -m pip install --upgrade git+https://github.com/ecmwf/pyfdb.git@master
```

&nbsp;
## 2. Example

An example of archival, listing and retrieval via pyfdb is shown next. For the example to work, FDB5 must be installed in the system, as well as the shutil, eccodes-python and pyfdb python packages. The GRIB files involved can be found under the `tests/unit/` folder in the pyfdb Git repository (https://github.com/ecmwf/pyfdb).

### Initialising FDB
```python
import pyfdb
import shutil

fdb = pyfdb.FDB()
```

### Archive
```python
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
fdb.archive(open(filename, "rb").read())

key['expver'] = 'xxxy'
filename = 'y138-400.grib'
fdb.archive(open(filename, "rb").read())
fdb.flush()
```

### List

#### direct function, request as dictionary
```python
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

for el in pyfdb.list(request, True, True):
    print(el['keys'])
# {'class': 'rd', 'date': '20191110', 'domain': 'g', 'expver': 'xxxx', 'stream': 'oper', 'time': '0000', 'levtype': 'pl', 'type': 'an', 'levelist': '300', 'param': '138', 'step': '0'}
```

#### direct function, updated dictionary
```python
request['levelist'] = ['100', '200', '300', '400', '500', '700', '850', '1000']
request['param'] = '138'

for el in pyfdb.list(request, True, True):
    print(el['keys'])
# {'class': 'rd', 'date': '20191110', 'domain': 'g', 'expver': 'xxxx', 'stream': 'oper', 'time': '0000', 'levtype': 'pl', 'type': 'an', 'levelist': '300', 'param': '138', 'step': '0'}
# {'class': 'rd', 'date': '20191110', 'domain': 'g', 'expver': 'xxxx', 'stream': 'oper', 'time': '0000', 'levtype': 'pl', 'type': 'an', 'levelist': '400', 'param': '138', 'step': '0'}
```

#### fdb object, request as dicitonary
As an alternative, use the created FDB instance and start queries from there
```python
request['levelist'] = ['400', '500', '700', '850', '1000']
for el in fdb.list(request, True, True):
    print(el)
# {'class': 'rd', 'date': '20191110', 'domain': 'g', 'expver': 'xxxx', 'stream': 'oper', 'time': '0000', 'levtype': 'pl', 'type': 'an', 'levelist': '400', 'param': '138', 'step': '0'}
```

### Retrieve

#### save to file
```python
import tempfile
import os

dir = tempfile.gettempdir()

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

filename = os.path.join(dir, 'x138-300bis.grib')
with open(filename, 'wb') as o, fdb.retrieve(request) as i:
    shutil.copyfileobj(i, o)

request['levelist'] = '400'
filename = os.path.join(dir, 'x138-400bis.grib')
with open(filename, 'wb') as o, fdb.retrieve(request) as i:
    shutil.copyfileobj(i, o)

request['expver'] = 'xxxy'
filename = os.path.join(dir, 'y138-400bis.grib')
with open(filename, 'wb') as o, pyfdb.retrieve(request) as i:
    shutil.copyfileobj(i, o)
```

#### read into python object
```python
datareader = pyfdb.retrieve(request)

# reading a small chunk
chunk = datareader.read(10)

print(chunk)
# bytearray(b'GRIB2\x0e\x0e\x01\x00\x00')

print('tell()', datareader.tell())
# tell() 10

# go back (partially) - seek(2)
datareader.seek(2)
print('tell()', datareader.tell())
# tell() 2

# reading a larger chunk
chunk = datareader.read(40)
print(chunk)
# bytearray(b'IB2\x0e\x0e\x01\x00\x004\x80b\x96\xff\x80\x8ad\x01\x90\x13\x0b\n\x00\x00\x01\x00\x00\x00\x00\x00\x00\x15\x00\x00\x00\x00\x00\x00\x00\x00\x00')

# go back - seek(0)
datareader.seek(0)
```

#### decode GRIB
```python
from eccodes import StreamReader
reader = StreamReader(datareader)
grib = next(reader)
grib.dump()
# [...redacted...]
```
