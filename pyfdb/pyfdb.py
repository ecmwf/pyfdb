#
# Copyright 2017-2018 European Centre for Medium-Range Weather Forecasts (ECMWF).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os

import cffi
import findlibs
from pkg_resources import parse_version

__version__ = "0.0.4"

__fdb_version__ = "5.11.0"

ffi = cffi.FFI()


class FDBException(RuntimeError):
    pass


class PatchedLib:
    """
    Patch a CFFI library with error handling

    Finds the header file associated with the FDB C API and parses it, loads the shared library,
    and patches the accessors with automatic python-C error handling.
    """

    __type_names = {}

    def __init__(self):

        libName = findlibs.find("fdb5")

        if libName is None:
            raise RuntimeError("FDB5 library not found")

        ffi.cdef(self.__read_header())
        self.__lib = ffi.dlopen(libName)

        # Todo: Version check against __version__

        # All of the executable members of the CFFI-loaded library are functions in the FDB
        # C API. These should be wrapped with the correct error handling. Otherwise forward
        # these on directly.

        for f in dir(self.__lib):
            try:
                attr = getattr(self.__lib, f)
                setattr(self, f, self.__check_error(attr, f) if callable(attr) else attr)
            except Exception as e:
                print(e)
                print("Error retrieving attribute", f, "from library")

        # Initialise the library, and set it up for python-appropriate behaviour

        self.fdb_initialise()

        # Check the library version

        tmp_str = ffi.new("char**")
        self.fdb_version(tmp_str)
        versionstr = ffi.string(tmp_str[0]).decode("utf-8")

        if parse_version(versionstr) < parse_version(__fdb_version__):
            raise RuntimeError("Version of libfdb found is too old. {} < {}".format(versionstr, __fdb_version__))

    def __read_header(self):
        with open(os.path.join(os.path.dirname(__file__), "processed_fdb.h"), "r") as f:
            return f.read()

    def __check_error(self, fn, name):
        """
        If calls into the FDB library return errors, ensure that they get detected and reported
        by throwing an appropriate python exception.
        """

        def wrapped_fn(*args, **kwargs):
            retval = fn(*args, **kwargs)
            if retval != self.__lib.FDB_SUCCESS and retval != self.__lib.FDB_ITERATION_COMPLETE:
                error_str = "Error in function {}: {}".format(
                    name, ffi.string(self.__lib.fdb_error_string(retval)).decode()
                )
                raise FDBException(error_str)
            return retval

        return wrapped_fn


# Bootstrap the library

lib = PatchedLib()


class Key:
    __key = None

    def __init__(self, keys):
        key = ffi.new("fdb_key_t**")
        lib.fdb_new_key(key)
        # Set free function
        self.__key = ffi.gc(key[0], lib.fdb_delete_key)

        for k, v in keys.items():
            self.set(k, v)

    def set(self, param, value):
        lib.fdb_key_add(
            self.__key, ffi.new("const char[]", param.encode("ascii")), ffi.new("const char[]", value.encode("ascii"))
        )

    @property
    def ctype(self):
        return self.__key


class Request:
    __request = None

    def __init__(self, request):
        newrequest = ffi.new("fdb_request_t**")

        # we assume a retrieve request represented as a dictionary
        lib.fdb_new_request(newrequest)
        self.__request = ffi.gc(newrequest[0], lib.fdb_delete_request)

        for name, values in request.items():
            self.value(name, values)

    def value(self, name, values):
        if name and name != "verb":
            cvals = []
            if isinstance(values, (str, int)):
                values = [values]
            for value in values:
                if isinstance(value, int):
                    value = str(value)
                cval = ffi.new("const char[]", value.encode("ascii"))
                cvals.append(cval)

            lib.fdb_request_add(
                self.__request,
                ffi.new("const char[]", name.encode("ascii")),
                ffi.new("const char*[]", cvals),
                len(values),
            )

    def expand(self):
        lib.fdb_expand_request(self.__request)

    @property
    def ctype(self):
        return self.__request


class ListIterator:
    __iterator = None
    __key = False

    def __init__(self, fdb, request, duplicates, key=False, expand=True, depth=3):
        iterator = ffi.new("fdb_listiterator_t**")
        if request:
            req = Request(request)
            if expand:
                req.expand()
            lib.fdb_list(fdb.ctype, req.ctype, iterator, duplicates, depth)
        else:
            lib.fdb_list(fdb.ctype, ffi.NULL, iterator, duplicates, depth)

        self.__iterator = ffi.gc(iterator[0], lib.fdb_delete_listiterator)
        self.__key = key

    def __iter__(self):

        path = ffi.new("const char**")
        off = ffi.new("size_t*")
        len = ffi.new("size_t*")

        err = 0
        while err == 0:
            err = lib.fdb_listiterator_next(self.__iterator)
            if err == 0:

                lib.fdb_listiterator_attrs(self.__iterator, path, off, len)
                el = dict(path=ffi.string(path[0]).decode("utf-8"), offset=off[0], length=len[0])

                if self.__key:
                    splitkey = ffi.new("fdb_split_key_t**")
                    lib.fdb_new_splitkey(splitkey)
                    key = ffi.gc(splitkey[0], lib.fdb_delete_splitkey)

                    lib.fdb_listiterator_splitkey(self.__iterator, key)

                    k = ffi.new("const char**")
                    v = ffi.new("const char**")
                    level = ffi.new("size_t*")

                    meta = dict()
                    while lib.fdb_splitkey_metadata(key, k, v, level) == 0:
                        meta[ffi.string(k[0]).decode("utf-8")] = ffi.string(v[0]).decode("utf-8")
                        lib.fdb_splitkey_next(key)
                    el["keys"] = meta

                yield el


class DataRetriever:
    __dataread = None
    __opened = False

    def __init__(self, fdb, request, expand=True):
        dataread = ffi.new("fdb_datareader_t **")
        lib.fdb_new_datareader(dataread)
        self.__dataread = ffi.gc(dataread[0], lib.fdb_delete_datareader)
        req = Request(request)
        if expand:
            req.expand()
        lib.fdb_retrieve(fdb.ctype, req.ctype, self.__dataread)

    mode = "rb"

    def open(self):
        if not self.__opened:
            self.__opened = True
            lib.fdb_datareader_open(self.__dataread, ffi.NULL)

    def close(self):
        if self.__opened:
            self.__opened = False
            lib.fdb_datareader_close(self.__dataread)

    def skip(self, count):
        self.open()
        if isinstance(count, int):
            lib.fdb_datareader_skip(self.__dataread, count)

    def seek(self, where):
        self.open()
        if isinstance(where, int):
            lib.fdb_datareader_seek(self.__dataread, where)

    def tell(self):
        self.open()
        where = ffi.new("long*")
        lib.fdb_datareader_tell(self.__dataread, where)
        return where[0]

    def read(self, count):
        self.open()
        if isinstance(count, int):
            buf = bytearray(count)
            read = ffi.new("long*")
            lib.fdb_datareader_read(self.__dataread, ffi.from_buffer(buf), count, read)
            return buf[0 : read[0]]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        None


class FDB:
    """This is the main container class for accessing FDB"""

    __fdb = None

    def __init__(self):
        fdb = ffi.new("fdb_handle_t**")
        lib.fdb_new_handle(fdb)

        # Set free function
        self.__fdb = ffi.gc(fdb[0], lib.fdb_delete_handle)

    def archive(self, data, request=None):
        if request is None:
            lib.fdb_archive_multiple(self.ctype, ffi.NULL, ffi.from_buffer(data), len(data))
        else:
            lib.fdb_archive_multiple(self.ctype, Request(request).ctype, ffi.from_buffer(data), len(data))

    def flush(self):
        lib.fdb_flush(self.ctype)

    def list(self, request=None, duplicates=False, keys=False, expand=True, depth=3):
        return ListIterator(self, request, duplicates, keys, expand, depth)

    def retrieve(self, request):
        return DataRetriever(self, request)

    @property
    def ctype(self):
        return self.__fdb


fdb = None


def archive(data):
    global fdb
    if not fdb:
        fdb = FDB()
    fdb.archive(data)


def list(request, duplicates=False, keys=False, expand=True, depth=3):
    global fdb
    if not fdb:
        fdb = FDB()
    return ListIterator(fdb, request, duplicates, keys, expand, depth)


def retrieve(request):
    global fdb
    if not fdb:
        fdb = FDB()
    return DataRetriever(fdb, request)


def flush():
    global fdb
    if not fdb:
        fdb = FDB()
    return fdb.flush()
