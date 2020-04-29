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
