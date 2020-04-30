import pyfdb

request = {
   'verb': 'retrieve',
   'class': 'rd',
   'expver': 'xxxy',
   'stream': 'oper',
   'date': '20191110',
   'time': '0000',
   'domain': 'g',
   'type': 'an',
   'levtype': 'pl',
   'step': '0',
   'levelist': ['300', '400', '500'],
   'param': ['138', '155']
}
print('direct function, request as dictionary:', request)
for el in pyfdb.list(request):
    print(el)

request['levelist'] = ['100', '200', '300', '400', '500', '700', '850', '1000']
request['param'] = '138'
print('direct function, updated dictionary:', request)
for el in pyfdb.list(request):
    print(el)

requeststring = 'class=rd,expver=xxxy'
print('direct function, request as string:', requeststring)
for el in pyfdb.list(requeststring):
    print(el)


# as an alternative, create a FDB instance and start queries from there
fdb = pyfdb.FDB()
print('fdb object, request as dictionary:', request)
for el in fdb.list(request):
    print(el)

requeststring = 'class=rd,expver=xxxx,levelist=300/to/500'
print('fdb object, request as string:', requeststring)
for el in fdb.list(requeststring):
    print(el)


print('\n\nFDB retrieve')
print('fdb object, retrieve from request:', request)
filename = 'foo.grib'
print('and save to file ', filename)
fdb.retrieve(request).saveTo(open(filename, 'wb'))


print('\ndirect function, retrieve from request:', request)
data = pyfdb.retrieve(request)

print('\nreading a small chunk')
chunk = data.read(40)
print(chunk)
print('return to data start - seek(0)')
data.seek(0)
print('reading a larger chunk')
chunk = data.read(100)
print(chunk)
#
# print(data.tell())
filename = 'bar.grib'
print('\nsave to file ', filename)
data.saveTo(open(filename, 'wb'))