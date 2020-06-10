import pyfdb
import shutil

# using minparams - not full request

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
    'levelist': [300, '400', 500],
    'param': ['138', 155, 't']
}
print('direct function, request as dictionary:', request)
for el in pyfdb.list(request):
    print(el)

request['levelist'] = ['100', '200', '300', '400', '500', '700', '850', '1000']
request['param'] = '138'
print('\ndirect function, updated dictionary:', request)
for el in pyfdb.list(request):
    print(el)

requeststring = 'class=rd,expver=xxxy'
print('\ndirect function, request as string:', requeststring)
for el in pyfdb.list(requeststring):
    print(el)


# as an alternative, create a FDB instance and start queries from there
request['levelist'] = ['400', '500', '700', '850', '1000']
fdb = pyfdb.FDB()
print('\nfdb object, request as dictionary:', request)
for el in fdb.list(request):
    print(el)

requeststring = 'class=rd,expver=xxxx,levelist=300/to/500'
print('\nfdb object, request as string:', requeststring)
for el in fdb.list(requeststring):
    print(el)

print('\nlist ALL:')
for el in fdb.list():
    print(el)

### Retrieve ###

request = {
    'class': 'od',
    'expver': '0001',
    'stream': 'oper',
    'date': '20040118',
    'time': '0000',
    'domain': 'g',
    'type': 'an',
    'levtype': 'sfc',
    'step': 0,
    'param': 151
}
print('\n\nFDB retrieve')
print('direct function, retrieve from request:', request)
datareader = pyfdb.retrieve(request)

print('\nreading a small chunk')
chunk = datareader.read(10)
print(chunk)
print('tell()', datareader.tell())

print('go back (partially) - seek(2)')
datareader.seek(2)
print('tell()', datareader.tell())

print('reading a larger chunk')
chunk = datareader.read(40)
print(chunk)

from pyeccodes import Reader

print('go back - seek(0), and decode GRIB')
datareader.seek(0)
reader = Reader(datareader)
grib = next(reader)
grib.dump()


filename = 'foo.grib'
print('\nsave to file ', filename)
with open(filename, 'wb') as o, fdb.retrieve(request) as i:
    shutil.copyfileobj(i, o)

# fdb.archive()