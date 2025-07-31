# (C) Copyright 2011- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.

import builtins
import io
import json
import os
from functools import wraps
from typing import Optional, overload

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
                    name,
                    ffi.string(self.__lib.fdb_error_string(retval)).decode("utf-8", "backslashreplace"),
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

    def __init__(self, keys: dict[str, str]):
        key = ffi.new("fdb_key_t**")
        lib.fdb_new_key(key)
        # Set free function
        self.__key = ffi.gc(key[0], lib.fdb_delete_key)

        for k, v in keys.items():
            self.set(k, v)

    def set(self, param: str, value: str):
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

    def __init__(self, fdb, request, duplicates, key=False, expand=True, schema=False):
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
        self.__schema = schema

        self.path = ffi.new("const char**")
        self.off = ffi.new("size_t*")
        self.len = ffi.new("size_t*")
        

    def __next__(self) -> dict:
        err = lib.fdb_listiterator_next(self.__iterator)

        if err != 0:
            raise StopIteration

        lib.fdb_listiterator_attrs(self.__iterator, self.path, self.off, self.len)
        el = dict(
            path=ffi.string(self.path[0]).decode("utf-8"),
            offset=self.off[0],
            length=self.len[0],
        )

        if self.__key or self.__schema:
            splitkey = ffi.new("fdb_split_key_t**")
            lib.fdb_new_splitkey(splitkey)
            key = ffi.gc(splitkey[0], lib.fdb_delete_splitkey)

            lib.fdb_listiterator_splitkey(self.__iterator, key)

            k = ffi.new("const char**")
            v = ffi.new("const char**")
            level = ffi.new("size_t*")

            meta = dict()
            if self.__schema:
                schema = dict()
                for lvl in range(1,4):
                    schema[lvl] = dict()
                while lib.fdb_splitkey_next_metadata(key, k, v, level) == 0:
                    mKey = ffi.string(k[0]).decode('utf-8')
                    val  = ffi.string(v[0]).decode('utf-8')
                    meta[mKey] = val
                    schema[int(level[0])+1][mKey] = val
                el['schema'] = schema
            else: # key=True and schema=False
                while lib.fdb_splitkey_next_metadata(key, k, v, level) == 0:
                    meta[ffi.string(k[0]).decode("utf-8")] = ffi.string(v[0]).decode("utf-8")

            if self.__key:
                el["keys"] = meta

        return el

    def __iter__(self):
        return self


class WipeIterator:
    __iterator = None

    def __init__(self, fdb, request, doit, porcelain, unsafeWipeAll):
        iterator = ffi.new("fdb_wipe_iterator_t**")
        req = Request(request)
        lib.fdb_wipe(fdb.ctype, req.ctype, doit, porcelain, unsafeWipeAll, iterator)
        self.__iterator = ffi.gc(iterator[0], lib.fdb_delete_wipe_iterator)

    def __iter__(self):
        element = ffi.new("fdb_wipe_element_t**")
        while lib.fdb_wipe_iterator_next(self.__iterator, element) == lib.FDB_SUCCESS:
            self.__element = ffi.gc(element[0], lib.fdb_delete_wipe_element)
            msg = ffi.new("const char**")
            lib.fdb_wipe_element_string(self.__element, msg)
            yield ffi.string(msg[0]).decode("utf-8")


class PurgeIterator:
    __iterator = None

    def __init__(self, fdb, request, doit, porcelain):
        iterator = ffi.new("fdb_purge_iterator_t**")
        req = Request(request)
        lib.fdb_purge(fdb.ctype, req.ctype, doit, porcelain, iterator)
        self.__iterator = ffi.gc(iterator[0], lib.fdb_delete_purge_iterator)

    def __iter__(self):
        element = ffi.new("fdb_purge_element_t**")
        while lib.fdb_purge_iterator_next(self.__iterator, element) == lib.FDB_SUCCESS:
            self.__element = ffi.gc(element[0], lib.fdb_delete_purge_element)
            msg = ffi.new("const char**")
            lib.fdb_purge_element_string(self.__element, msg)
            yield ffi.string(msg[0]).decode("utf-8")


class DataRetriever(io.RawIOBase):
    __dataread = None
    __opened = False

    def __init__(self, fdb, request: dict[str, str], expand: bool = True):
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

    def size(self):
        size = ffi.new("long*")
        lib.fdb_datareader_size(self.__dataread, size)
        return size[0]

    def read(self, size=-1) -> bytes:
        self.open()
        if isinstance(size, int):
            if size == -1:
                size = self.size()
            buf = bytearray(size)
            read = ffi.new("long*")
            lib.fdb_datareader_read(self.__dataread, ffi.from_buffer(buf), size, read)
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

    @overload
    def archive(self, data: bytes, request: Optional[Request | dict | None] = None, key: None = None) -> None: ...

    @overload
    def archive(self, data: bytes, request: None = None, key: Optional[Key | dict] = None) -> None: ...

    def archive(
        self,
        data: bytes,
        request: Optional[Request | dict] = None,
        key: Optional[Key | dict] = None,
    ) -> None:
        """Archive data into the FDB5 database

        Args:
        -----
            data: bytes data to be archived
            request: Depending on the type, one of the following:
                Request:
                    Calls the fdb archive multiple API.
                    The given data will be checked against the
                    given request. In case of a mismatch an
                    exception will be raised.
                Dict[str, str]:
                    Dictionary representing the request to be
                    associated with the data,
                    if not provided the key will be constructed
                    from the data.
            key: Depending on the type, one of the following:
                Key:
                    Calls the fdb archive API. The key is used
                    to set the meta-information of the written
                    bytes. There is no check whether the written
                    bytes and the meta-information is matching.
                Dict[str, str]:
                    Dictionary representing the key to be associated with the data.

        Notes:
        ------
        If a key is specified, `data` is archived as a single element.
        """
        if request is not None and key is not None:
            raise RuntimeError(
                "request and key parameter are both None. Either set a request (exclusive) or a key for the given data."
            )

        if key is None:
            match request:
                case Request():
                    lib.fdb_archive_multiple(self.ctype, request.ctype, ffi.from_buffer(data), len(data))
                case builtins.dict():
                    lib.fdb_archive_multiple(
                        self.ctype,
                        Request(request).ctype,
                        ffi.from_buffer(data),
                        len(data),
                    )
                case None:
                    lib.fdb_archive_multiple(self.ctype, ffi.NULL, ffi.from_buffer(data), len(data))
                case _:
                    raise RuntimeError(
                        "Given request is neither a Request nor a dict[str, str]. \
                        Please provide a valid request or consider calling the function with the `key` argument."
                    )

        if key:
            match key:
                case Key():
                    lib.fdb_archive(self.ctype, key.ctype, data, len(data))
                case builtins.dict():
                    lib.fdb_archive(self.ctype, Key(key).ctype, ffi.from_buffer(data), len(data))
                case _:
                    raise RuntimeError(
                        "Given request is neither a Key nor a dict[str, str]. \
                        Please provide a valid request or consider calling the function with the `request` argument."
                    )

    def flush(self) -> None:
        """Flush any archived data to disk"""
        lib.fdb_flush(self.ctype)

    def list(self, request=None, duplicates=False, keys=False, schema=False) -> ListIterator:
        """List entries in the FDB5 database

        Args:
            request (dict): dictionary representing the request.
            duplicates (bool) = false : whether to include duplicate entries.
            keys (bool) = false : whether to include the keys for each entry in the output.
            schema (bool) = false : whether to include the metadata sorted according to the three FDB schema levels for each entry in the output.

        Returns:
            ListIterator: an iterator over the entries.
        """
        return ListIterator(self, request, duplicates, keys, schema)

    def retrieve(self, request) -> DataRetriever:
        """Retrieve data as a stream.

        Args:
            request (dict): dictionary representing the request.

        Returns:
            DataRetriever: An object implementing a file-like interface to the data stream.
        """
        return DataRetriever(self, request)

    # @todo: I believe unsafeWipeAll may do *more* than just allowing deletion of non-FDB files.
    # but it is not documented anywhere.
    def wipe(self, request, doit=False, porcelain=False, unsafeWipeAll=False, verbose=False):
        """Delete data matching the request from the FDB.

        This function identifies all entries in the database that match the given request.
        If `doit` is False, a dry run is performed and only the entries that *would* be deleted are printed.
        If `doit` is True, the matching entries are actually removed from the database.

        Args:
            request (dict): Dictionary representing the request.
            doit (bool, optional): If True, performs the wipe (deletes matching entries). If False, performs a dry
              run and prints the entries that would be deleted.
            porcelain (bool, optional): If True, prints the output of the wipe operation in a more machine-readable
              format.
            unsafeWipeAll (bool, optional): If True, also delete non-FDB files found in the database directory.
            verbose (bool, optional): If True, prints the output of the wipe operation even if `doit` is True.
        """

        for msg in WipeIterator(self, request, doit, porcelain, unsafeWipeAll):
            if verbose or not doit:
                print(msg)

        # @note: It would be nice to return the URIs of the deleted files, but the output of the WipeIterator is not
        #  structured in a way that reliably allows this.
        return

    def purge(self, request, doit=False, porcelain=False, verbose=False):
        """Delete *duplicate* data matching the request from the FDB. Only the newest version of the data is kept.

        This function identifies all entries in the database that match the given request.
        If `doit` is False, a dry run is performed and only the entries that *would* be deleted are printed.
        If `doit` is True, the matching entries are actually removed from the database.

        Args:
            request (dict): Dictionary representing the request.
            doit (bool, optional): If True, performs the purge (deletes matching entries). If False, performs a dry
              run and prints the entries that would be deleted.
            porcelain (bool, optional): If True, prints the output of the purge operation in a more machine-readable
              format.
            verbose (bool, optional): If True, prints the output of the purge operation even if `doit` is True.
        """
        for msg in PurgeIterator(self, request, doit, porcelain):
            if verbose or not doit:
                print(msg)

        return

    @property
    def ctype(self):
        return self.__fdb


fdb = None


# Use functools.wraps to copy over the docstring from FDB.xxx to the module level functions
@wraps(FDB.archive)
def archive(
    data: bytes,
    request: Optional[Request | dict] = None,
    key: Optional[Key | dict] = None,
) -> None:
    """Archives bytes to the FDB

    Args:
        data: data in bytes
        request_or_key: Depending on the type the following functions are triggered:
            Key:

    """
    global fdb
    if not fdb:
        fdb = FDB()
    fdb.archive(data, request=request, key=key)


@wraps(FDB.list)
def list(request, duplicates=False, keys=False, schema=False) -> ListIterator:
    global fdb
    if not fdb:
        fdb = FDB()
    return ListIterator(fdb, request, duplicates, keys, schema)


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
