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
import io
import json
import os
from functools import wraps

import cffi
import findlibs
from packaging import version

from .version import __version__

__fdb_version__ = "5.12.1"

ffi = cffi.FFI()


class FDBException(RuntimeError):
    pass


class PatchedLib:
    """
    Patch a CFFI library with error handling

    Finds the header file associated with the FDB C API and parses it, loads the shared library,
    and patches the accessors with automatic python-C error handling.
    """

    def __init__(self):
        self.path = findlibs.find("fdb5")

        if self.path is None:
            raise RuntimeError("FDB5 library not found")

        ffi.cdef(self.__read_header())
        self.__lib = ffi.dlopen(self.path)

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
        self.version = ffi.string(tmp_str[0]).decode("utf-8")

        if version.parse(self.version) < version.parse(__fdb_version__):
            raise RuntimeError(
                f"This version of pyfdb ({__version__}) requires fdb version {__fdb_version__} or greater."
                f"You have fdb version {self.version} loaded from {self.path}"
            )

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
                    name, ffi.string(self.__lib.fdb_error_string(retval)).decode("utf-8", "backslashreplace")
                )
                raise FDBException(error_str)
            return retval

        return wrapped_fn

    def __repr__(self):
        return f"<pyfdb.pyfdb.PatchedLib FDB5 version {self.version} from {self.path}>"


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
            self.__key,
            ffi.new("const char[]", param.encode("ascii")),
            ffi.new("const char[]", value.encode("ascii")),
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

    def __init__(self, fdb, request, duplicates, key=False, expand=True):
        iterator = ffi.new("fdb_listiterator_t**")
        if request:
            req = Request(request)
            if expand:
                req.expand()
            lib.fdb_list(fdb.ctype, req.ctype, iterator, duplicates)
        else:
            lib.fdb_list(fdb.ctype, ffi.NULL, iterator, duplicates)

        self.__iterator = ffi.gc(iterator[0], lib.fdb_delete_listiterator)
        self.__key = key

        self.path = ffi.new("const char**")
        self.off = ffi.new("size_t*")
        self.len = ffi.new("size_t*")

    def __next__(self) -> dict:
        err = lib.fdb_listiterator_next(self.__iterator)

        if err != 0:
            raise StopIteration

        lib.fdb_listiterator_attrs(self.__iterator, self.path, self.off, self.len)
        el = dict(path=ffi.string(self.path[0]).decode("utf-8"), offset=self.off[0], length=self.len[0])

        if self.__key:
            splitkey = ffi.new("fdb_split_key_t**")
            lib.fdb_new_splitkey(splitkey)
            key = ffi.gc(splitkey[0], lib.fdb_delete_splitkey)

            lib.fdb_listiterator_splitkey(self.__iterator, key)

            k = ffi.new("const char**")
            v = ffi.new("const char**")
            level = ffi.new("size_t*")

            meta = dict()
            while lib.fdb_splitkey_next_metadata(key, k, v, level) == 0:
                meta[ffi.string(k[0]).decode("utf-8")] = ffi.string(v[0]).decode("utf-8")
            el["keys"] = meta

        return el

    def __iter__(self):
        return self


class DataRetriever(io.RawIOBase):
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

    def seek(self, where, whence=io.SEEK_SET):
        if whence != io.SEEK_SET:
            raise NotImplementedError(
                f"SEEK_CUR and SEEK_END are not currently supported on {self.__class__.__name__} objects"
            )
        self.open()
        if isinstance(where, int):
            lib.fdb_datareader_seek(self.__dataread, where)

    def tell(self):
        self.open()
        where = ffi.new("long*")
        lib.fdb_datareader_tell(self.__dataread, where)
        return where[0]

    def read(self, count=-1) -> bytes:
        self.open()
        if isinstance(count, int):
            buf = bytearray(count)
            read = ffi.new("long*")
            lib.fdb_datareader_read(self.__dataread, ffi.from_buffer(buf), count, read)
            return buf[0 : read[0]]
        return bytearray()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        None


class FDB:
    """This is the main container class for accessing FDB

    Usage:
        fdb = pyfdb.FDB()
        # call fdb.archive, fdb.list, fdb.retrieve, fdb.flush as needed.

    See the module level pyfdb.list, pyfdb.retrieve, and pyfdb.archive
    docstrings for more information on these functions.
    """

    __fdb = None

    def __init__(self, config=None, user_config=None):
        fdb = ffi.new("fdb_handle_t**")

        if config is not None or user_config is not None:

            def prepare_config(c):
                if c is None:
                    return ""
                if not isinstance(c, str):
                    return json.dumps(c)
                return c

            config = prepare_config(config)
            user_config = prepare_config(user_config)

            lib.fdb_new_handle_from_yaml(
                fdb,
                ffi.new("const char[]", config.encode("utf-8")),
                ffi.new("const char[]", user_config.encode("utf-8")),
            )
        else:
            lib.fdb_new_handle(fdb)

        # Set free function
        self.__fdb = ffi.gc(fdb[0], lib.fdb_delete_handle)

    def archive(self, data, request=None) -> None:
        """Archive data into the FDB5 database

        Args:
            data (bytes): bytes data to be archived
            request (dict | None): dictionary representing the request to be associated with the data,
                if not provided the key will be constructed from the data.
        """
        if request is None:
            lib.fdb_archive_multiple(self.ctype, ffi.NULL, ffi.from_buffer(data), len(data))
        else:
            lib.fdb_archive_multiple(self.ctype, Request(request).ctype, ffi.from_buffer(data), len(data))

    def flush(self) -> None:
        """Flush any archived data to disk"""
        lib.fdb_flush(self.ctype)

    def list(self, request=None, duplicates=False, keys=False) -> ListIterator:
        """List entries in the FDB5 database

        Args:
            request (dict): dictionary representing the request.
            duplicates (bool) = false : whether to include duplicate entries.
            keys (bool) = false : whether to include the keys for each entry in the output.

        Returns:
            ListIterator: an iterator over the entries.
        """
        return ListIterator(self, request, duplicates, keys)

    def retrieve(self, request) -> DataRetriever:
        """Retrieve data as a stream.

        Args:
            request (dict): dictionary representing the request.

        Returns:
            DataRetriever: An object implementing a file-like interface to the data stream.
        """
        return DataRetriever(self, request)

    @property
    def ctype(self):
        return self.__fdb


fdb = None


# Use functools.wraps to copy over the docstring from FDB.xxx to the module level functions
@wraps(FDB.archive)
def archive(data) -> None:
    global fdb
    if not fdb:
        fdb = FDB()
    fdb.archive(data)


@wraps(FDB.list)
def list(request, duplicates=False, keys=False) -> ListIterator:
    global fdb
    if not fdb:
        fdb = FDB()
    return ListIterator(fdb, request, duplicates, keys)


@wraps(FDB.retrieve)
def retrieve(request) -> DataRetriever:
    global fdb
    if not fdb:
        fdb = FDB()
    return DataRetriever(fdb, request)


@wraps(FDB.flush)
def flush():
    global fdb
    if not fdb:
        fdb = FDB()
    return fdb.flush()
