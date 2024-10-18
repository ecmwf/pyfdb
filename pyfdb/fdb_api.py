import os
from typing import List

import cffi
import findlibs
from packaging import version

__version__ = "0.0.4"
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


class FDBApi:

    @classmethod
    def get_gc_fdb_keys(cls):
        """
        Create a new `fdb_key_t**` object and return a garbage collected version
        of it.
        """
        key = ffi.new("fdb_key_t**")
        lib.fdb_new_key(key)
        # Set free function
        return ffi.gc(key[0], lib.fdb_delete_key)

    @classmethod
    def get_gc_splitkey(cls, splitkey):
        """
        Create a new `fdb_splitkey**` object and return a garbage collected version
        of it.
        """
        lib.fdb_new_splitkey(splitkey)
        return ffi.gc(splitkey[0], lib.fdb_delete_splitkey)

    @classmethod
    def add_fdb_key(cls, fdb_keys, param: str, value: str):
        """
        Takes a new `fdb_key_t**` object, a parameter and a value and adds it
        as its keys.

        param and value are ascii encode during the function call
        -----------------------
        e.g.
        self.__key = FDBApi.get_gc_fdb_keys()
        FDBApi.add_fdb_key(self.__key, "param", "value")

        """
        lib.fdb_key_add(
            fdb_keys,
            ffi.new("const char[]", param.encode("ascii")),
            ffi.new("const char[]", value.encode("ascii")),
        )

    @classmethod
    def get_gc_fdb_request(cls):
        """
        Create a new `fdb_key_t**` object and return a garbage collected version
        of it.
        """
        newrequest = ffi.new("fdb_request_t**")

        # we assume a retrieve request represented as a dictionary
        lib.fdb_new_request(newrequest)
        return ffi.gc(newrequest[0], lib.fdb_delete_request)

    @classmethod
    def add_fdb_request(cls, requests, name: str, cvals: List, num_vals: int):
        """
        Takes a new `fdb_request_t**` object, a name and a cvals and adds it
        as its keys.

        param and value are ascii encode during the function call
        -----------------------
        e.g.

        self.__request = FDBApi.get_gc_fdb_request()
        FDBApi.add_fdb_request(self.__request, name, cvals, len(values))
        """
        lib.fdb_request_add(
            requests,
            ffi.new("const char[]", name.encode("ascii")),
            ffi.new("const char*[]", cvals),
            num_vals,
        )

    @classmethod
    def fdb_list(cls, fdb, request, iterator, duplicates):
        lib.fdb_list(fdb.ctype, request.ctype, iterator, duplicates)

    @classmethod
    def fdb_retrieve(cls, fdb, request, dataread):
        lib.fdb_retrieve(fdb.ctype, request.ctype, dataread)

    @classmethod
    def fdb_list_no_request(cls, fdb, iterator, duplicates):
        lib.fdb_list(fdb.ctype, ffi.NULL, iterator, duplicates)

    @classmethod
    def register_gc_fdb_iterator(cls, iterator):
        return ffi.gc(iterator[0], lib.fdb_delete_listiterator)

    @classmethod
    def create_ffi_const_char_ascii_encoded(cls, value):
        """
        Create a new const char[] for a given value, encoded as ascii
        """
        return ffi.new("const char[]", value.encode("ascii"))

    @classmethod
    def create_ffi_const_char_utf8_encoded(cls, value):
        """
        Create a new const char[] for a given value, encoded as ascii
        """
        return ffi.new("const char[]", value.encode("utf-8"))

    @classmethod
    def create_fdb_handle_ptr_ptr(cls):
        """
        Create a new fdb_handle_ptr_ptr
        """
        return ffi.new("fdb_handle_t**")

    @classmethod
    def create_ffi_string_utf8_encoded(cls, value):
        """
        Create a new const char[] for a given value, encoded as ascii
        """
        return ffi.string(value).decode("utf-8")

    @classmethod
    def create_ffi_fdb_list_iterator(cls):
        """
        Create a new fdb_listiterator_t**
        """
        return ffi.new("fdb_listiterator_t**")

    @classmethod
    def create_ffi_const_char_ptr_ptr(cls):
        """
        Create a new const char**
        """
        return ffi.new("const char**")

    @classmethod
    def create_ffi_size_t_ptr(cls):
        """
        Create a new size_t*
        """
        return ffi.new("size_t*")

    @classmethod
    def create_gc_fdb_datareader_ptr(cls):
        """
        Create a new fdb_datareader_t**
        """
        dataread = ffi.new("fdb_datareader_t **")
        lib.fdb_new_datareader(dataread)
        return ffi.gc(dataread[0], lib.fdb_delete_datareader)

    @classmethod
    def fdb_datareader_open(cls, dataread):
        lib.fdb_datareader_open(dataread, ffi.NULL)

    @classmethod
    def fdb_datareader_close(cls, dataread):
        lib.fdb_datareader_close(dataread)

    @classmethod
    def fdb_datareader_skip(cls, dataread, count):
        lib.fdb_datareader_skip(dataread, count)

    @classmethod
    def fdb_datareader_seek(cls, dataread, where):
        lib.fdb_datareader_seek(dataread, where)

    @classmethod
    def fdb_datareader_read(cls, dataread, count=-1):
        if isinstance(count, int):
            buf = bytearray(count)
            read = ffi.new("long*")
            lib.fdb_datareader_read(dataread, ffi.from_buffer(buf), count, read)
            return buf[0 : read[0]]
        return bytearray()

    @classmethod
    def fdb_datareader_tell(cls, dataread):
        where = ffi.new("long*")
        lib.fdb_datareader_tell(dataread, where)
        return where[0]

    @classmethod
    def create_ffi_fdb_split_key_ptr_ptr(cls):
        """
        Create a new fdb_split_key_t** for
        """
        return ffi.new("fdb_split_key_t**")

    @classmethod
    def fdb_expand_request(cls, request):
        """
        Forwards call to `PatchedLib::fdb_expand_request` method.
        """
        lib.fdb_expand_request(request)

    @classmethod
    def fdb_iterator_next(cls, iterator):
        return lib.fdb_listiterator_next(iterator)

    @classmethod
    def fdb_listiterator_attrs(cls, iterator, path, off, len):
        lib.fdb_listiterator_attrs(iterator, path, off, len)

    @classmethod
    def fdb_listiterator_splitkey(cls, iterator, key):
        lib.fdb_listiterator_splitkey(iterator, key)

    @classmethod
    def fdb_splitkey_next_metadata(cls, key, k, v, level):
        return lib.fdb_splitkey_next_metadata(key, k, v, level)

    @classmethod
    def fdb_new_handle_from_yaml(cls, fdb, config, user_config):
        lib.fdb_new_handle_from_yaml(
            fdb,
            FDBApi.create_ffi_const_char_utf8_encoded(config),
            FDBApi.create_ffi_const_char_utf8_encoded(user_config)
        )

    @classmethod
    def fdb_new_handle(cls, fdb):
        lib.fdb_new_handle(fdb)

    @classmethod
    def fdb_register_gc(cls, fdb):
        return ffi.gc(fdb[0], lib.fdb_delete_handle)

    @classmethod
    def fdb_archive_multiple_no_request(cls, fdb_ctype, data, len):
        lib.fdb_archive_multiple(fdb_ctype, ffi.NULL, ffi.from_buffer(data), len)

    @classmethod
    def fdb_archive_multiple(cls, fdb_ctype, request, data, len):
        lib.fdb_archive_multiple(
            fdb_ctype, Request(request).ctype, ffi.from_buffer(data), len
        )

    @classmethod
    def fdb_flush(cls, fdb_ctype):
        """Flush any archived data to disk"""
        lib.fdb_flush(fdb_ctype)



lib = PatchedLib()
