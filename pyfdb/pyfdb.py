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
                cval = FDBApi.create_ffi_const_char_ascii_encoded(value)
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
        iterator = FDBApi.create_ffi_fdb_list_iterator()

        if request:
            req = Request(request)
            if expand:
                req.expand()
            FDBApi.fdb_list(fdb, req, iterator, duplicates)
        else:
            FDBApi.fdb_list_no_request(fdb, iterator, duplicates)

        self.__iterator = FDBApi.register_gc_fdb_iterator(iterator)
        self.__key = key

        self.path = FDBApi.create_ffi_const_char_ptr_ptr()
        self.off = FDBApi.create_ffi_size_t_ptr()
        self.len = FDBApi.create_ffi_size_t_ptr()

    def __next__(self) -> dict:
        err = FDBApi.fdb_iterator_next(self.__iterator)

        if err != 0:
            raise StopIteration

        FDBApi.fdb_listiterator_attrs(self.__iterator, self.path, self.off, self.len)
        el = dict(path=FDBApi.create_ffi_string_utf8_encoded(self.path[0]), offset=self.off[0], length=self.len[0])

        if self.__key:
            splitkey = FDBApi.create_ffi_fdb_split_key_ptr_ptr()
            key = FDBApi.get_gc_splitkey(splitkey)

            FDBApi.fdb_listiterator_splitkey(self.__iterator, key)

            k = FDBApi.create_ffi_const_char_ptr_ptr()
            v = FDBApi.create_ffi_const_char_ptr_ptr()
            level = FDBApi.create_ffi_size_t_ptr()

            meta = dict()
            while FDBApi.fdb_splitkey_next_metadata(key, k, v, level) == 0:
                meta[FDBApi.create_ffi_string_utf8_encoded(k[0])] = FDBApi.create_ffi_string_utf8_encoded(v[0])
            el["keys"] = meta

        return el

    def __iter__(self):
        return self


class DataRetriever(io.RawIOBase):
    __dataread = None
    __opened = False

    mode = "rb"

    def __init__(self, fdb, request, expand=True):
        self.__dataread = FDBApi.create_gc_fdb_datareader_ptr()
        request = Request(request)
        if expand:
            request.expand()
        FDBApi.fdb_retrieve(fdb, request, self.__dataread)

    def open(self):
        if not self.__opened:
            self.__opened = True
            FDBApi.fdb_datareader_open(self.__dataread)

    def close(self):
        if self.__opened:
            self.__opened = False
            FDBApi.fdb_datareader_close(self.__dataread)

    def skip(self, count):
        self.open()
        if isinstance(count, int):
            FDBApi.fdb_datareader_skip(self.__dataread, count)

    def seek(self, where, whence=io.SEEK_SET):
        if whence != io.SEEK_SET:
            raise NotImplementedError(
                f"SEEK_CUR and SEEK_END are not currently supported on {self.__class__.__name__} objects"
            )
        self.open()
        if isinstance(where, int):
            FDBApi.fdb_datareader_seek(self.__dataread, where)

    def tell(self):
        self.open()
        return FDBApi.fdb_datareader_tell(self.__dataread)

    def read(self, count=-1) -> bytes:
        self.open()
        return FDBApi.fdb_datareader_read(self.__dataread, count)

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
        fdb = FDBApi.create_fdb_handle_ptr_ptr()

        if config is not None or user_config is not None:

            def prepare_config(c):
                if c is None:
                    return ""
                if not isinstance(c, str):
                    return json.dumps(c)
                return c

            config = prepare_config(config)
            user_config = prepare_config(user_config)

            FDBApi.fdb_new_handle_from_yaml(
                fdb,
                config,
                user_config
            )
        else:
            FDBApi.fdb_new_handle(fdb)

        # Set free function
        self.__fdb = FDBApi.fdb_register_gc(fdb)

    def archive(self, data, request=None) -> None:
        """Archive data into the FDB5 database

        Args:
            data (bytes): bytes data to be archived
            request (dict | None): dictionary representing the request to be associated with the data,
                if not provided the key will be constructed from the data.
        """
        if request is None:
            FDBApi.fdb_archive_multiple_no_request(self.ctype, data, len(data))
        else:
            FDBApi.fdb_archive_multiple(self.ctype, Request(request).ctype, data, len(data))

    def flush(self) -> None:
        """Flush any archived data to disk"""
        FDBApi.fdb_flush(self.ctype)

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
