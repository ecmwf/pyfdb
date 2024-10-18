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
from functools import wraps

from pyfdb import fdb_api
from pyfdb.fdb_api import FDBApi


class Key:
    __key = None

    def __init__(self, keys):
        self.__key = FDBApi.get_gc_fdb_keys()

        for k, v in keys.items():
            self.set(k, v)

    def set(self, param, value):
        FDBApi.add_fdb_key(self.__key, param, value)

    @property
    def ctype(self):
        return self.__key


class Request:
    __request = None

    def __init__(self, request):
        self.__request = FDBApi.get_gc_fdb_request()

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
                cval = FDBApi.create_ffi_string_ascii_encoded(value)
                cvals.append(cval)

            FDBApi.add_fdb_request(self.__request, name, cvals, len(values))

    def expand(self):
        FDBApi.fdb_expand_request(self.__request)

    @property
    def ctype(self):
        return self.__request


class ListIterator:
    __iterator = None
    __key = False

    def __init__(self, fdb, request, duplicates, key=False, expand=True):
        iterator = fdb_api.ffi.new("fdb_listiterator_t**")
        if request:
            req = Request(request)
            if expand:
                req.expand()
            fdb_api.lib.fdb_list(fdb.ctype, req.ctype, iterator, duplicates)
        else:
            fdb_api.lib.fdb_list(fdb.ctype, fdb_api.ffi.NULL, iterator, duplicates)

        self.__iterator = fdb_api.ffi.gc(iterator[0], fdb_api.lib.fdb_delete_listiterator)
        self.__key = key

        self.path = fdb_api.ffi.new("const char**")
        self.off = fdb_api.ffi.new("size_t*")
        self.len = fdb_api.ffi.new("size_t*")

    def __next__(self) -> dict:
        err = fdb_api.lib.fdb_listiterator_next(self.__iterator)

        if err != 0:
            raise StopIteration

        fdb_api.lib.fdb_listiterator_attrs(self.__iterator, self.path, self.off, self.len)
        el = dict(path=fdb_api.ffi.string(self.path[0]).decode("utf-8"), offset=self.off[0], length=self.len[0])

        if self.__key:
            splitkey = fdb_api.ffi.new("fdb_split_key_t**")
            fdb_api.lib.fdb_new_splitkey(splitkey)
            key = fdb_api.ffi.gc(splitkey[0], fdb_api.lib.fdb_delete_splitkey)

            fdb_api.lib.fdb_listiterator_splitkey(self.__iterator, key)

            k = fdb_api.ffi.new("const char**")
            v = fdb_api.ffi.new("const char**")
            level = fdb_api.ffi.new("size_t*")

            meta = dict()
            while fdb_api.lib.fdb_splitkey_next_metadata(key, k, v, level) == 0:
                meta[fdb_api.ffi.string(k[0]).decode("utf-8")] = fdb_api.ffi.string(v[0]).decode("utf-8")
            el["keys"] = meta

        return el

    def __iter__(self):
        return self


class DataRetriever(io.RawIOBase):
    __dataread = None
    __opened = False

    def __init__(self, fdb, request, expand=True):
        dataread = fdb_api.ffi.new("fdb_datareader_t **")
        fdb_api.lib.fdb_new_datareader(dataread)
        self.__dataread = fdb_api.ffi.gc(dataread[0], fdb_api.lib.fdb_delete_datareader)
        req = Request(request)
        if expand:
            req.expand()
        fdb_api.lib.fdb_retrieve(fdb.ctype, req.ctype, self.__dataread)

    mode = "rb"

    def open(self):
        if not self.__opened:
            self.__opened = True
            fdb_api.lib.fdb_datareader_open(self.__dataread, fdb_api.ffi.NULL)

    def close(self):
        if self.__opened:
            self.__opened = False
            fdb_api.lib.fdb_datareader_close(self.__dataread)

    def skip(self, count):
        self.open()
        if isinstance(count, int):
            fdb_api.lib.fdb_datareader_skip(self.__dataread, count)

    def seek(self, where, whence=io.SEEK_SET):
        if whence != io.SEEK_SET:
            raise NotImplementedError(
                f"SEEK_CUR and SEEK_END are not currently supported on {self.__class__.__name__} objects"
            )
        self.open()
        if isinstance(where, int):
            fdb_api.lib.fdb_datareader_seek(self.__dataread, where)

    def tell(self):
        self.open()
        where = fdb_api.ffi.new("long*")
        fdb_api.lib.fdb_datareader_tell(self.__dataread, where)
        return where[0]

    def read(self, count=-1) -> bytes:
        self.open()
        if isinstance(count, int):
            buf = bytearray(count)
            read = fdb_api.ffi.new("long*")
            fdb_api.lib.fdb_datareader_read(self.__dataread, fdb_api.ffi.from_buffer(buf), count, read)
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
        fdb = fdb_api.ffi.new("fdb_handle_t**")

        if config is not None or user_config is not None:

            def prepare_config(c):
                if c is None:
                    return ""
                if not isinstance(c, str):
                    return json.dumps(c)
                return c

            config = prepare_config(config)
            user_config = prepare_config(user_config)

            fdb_api.lib.fdb_new_handle_from_yaml(
                fdb,
                fdb_api.ffi.new("const char[]", config.encode("utf-8")),
                fdb_api.ffi.new("const char[]", user_config.encode("utf-8")),
            )
        else:
            fdb_api.lib.fdb_new_handle(fdb)

        # Set free function
        self.__fdb = fdb_api.ffi.gc(fdb[0], fdb_api.lib.fdb_delete_handle)

    def archive(self, data, request=None) -> None:
        """Archive data into the FDB5 database

        Args:
            data (bytes): bytes data to be archived
            request (dict | None): dictionary representing the request to be associated with the data,
                if not provided the key will be constructed from the data.
        """
        if request is None:
            fdb_api.lib.fdb_archive_multiple(self.ctype, fdb_api.ffi.NULL, fdb_api.ffi.from_buffer(data), len(data))
        else:
            fdb_api.lib.fdb_archive_multiple(
                self.ctype, Request(request).ctype, fdb_api.ffi.from_buffer(data), len(data)
            )

    def flush(self) -> None:
        """Flush any archived data to disk"""
        fdb_api.lib.fdb_flush(self.ctype)

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
