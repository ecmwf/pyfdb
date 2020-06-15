import pyfdb
import shutil

fdb = pyfdb.FDB()

### Archive ###
request = {
    'domain': 'g',
    'levtype': 'pl',
    'levelist': '300',
    'date': '20191110',
    'time': '0000',
    'step': '0',
    'param': '138',
    'class': 'rd',
    'type': 'an',
    'stream': 'oper',
    'expver': 'xxxx'
}

filename = 'x138-300.grib'
pyfdb.archive(request, open(filename, "rb").read())

request['levelist'] = '400'
filename = 'x138-400.grib'
pyfdb.archive(request, open(filename, "rb").read())

request['expver'] = 'xxxy'
filename = 'y138-400.grib'
fdb.archive(request, open(filename, "rb").read())


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
print('\n\ndirect function, updated dictionary:', request)
for el in pyfdb.list(request):
    print(el)

requeststring = 'class=rd,expver=xxxy'
print('\n\ndirect function, request as string:', requeststring)
for el in pyfdb.list(requeststring):
    print(el)


# as an alternative, create a FDB instance and start queries from there
request['levelist'] = ['400', '500', '700', '850', '1000']
print('\n\nfdb object, request as dictionary:', request)
for el in fdb.list(request):
    print(el)

requeststring = 'class=rd,expver=xxxx,levelist=300/to/500'
print('\n\nfdb object, request as string:', requeststring)
for el in fdb.list(requeststring):
    print(el)

print('\n\nlist ALL:')
for el in fdb.list():
    print(el)



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
print('\n\nsave to file ', filename)
with open(filename, 'wb') as o, fdb.retrieve(request) as i:
    shutil.copyfileobj(i, o)

request['levelist'] = '400'
filename = 'x138-400bis.grib'
print('\n\nsave to file ', filename)
with open(filename, 'wb') as o, fdb.retrieve(request) as i:
    shutil.copyfileobj(i, o)

request['expver'] = 'xxxy'
filename = 'y138-400bis.grib'
print('\n\nsave to file ', filename)
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

request['levelist'] = [300, '400']
request['expver'] = 'xxxx'
filename = 'foo.grib'
print('\nsave to file ', filename)
with open(filename, 'wb') as o, fdb.retrieve(request) as i:
    shutil.copyfileobj(i, o)

