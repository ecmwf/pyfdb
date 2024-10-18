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
    def create_ffi_string_ascii_encoded(cls, value):
        """
        Create a new const char[] for a given value, encoded as ascii
        """
        return ffi.new("const char[]", value.encode("ascii"))

    @classmethod
    def fdb_expand_request(cls, request):
        """
        Forwards call to `PatchedLib::fdb_expand_request` method.
        """
        lib.fdb_expand_request(request)


lib = PatchedLib()
