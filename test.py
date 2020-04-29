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
from collections import Iterable

import cffi
import pandas
import numpy
import io
import os
from functools import reduce
from pkg_resources import parse_version
from enum import IntEnum, unique

__version__ = '0.0.1'

__fdc_version__ = "5.6.0"

ffi = cffi.FFI()


class FDCException(RuntimeError):
    pass


class PatchedLib:
    """
    Patch a CFFI library with error handling

    Finds the header file associated with the FDB C API and parses it, loads the shared library,
    and patches the accessors with automatic python-C error handling.
    """
    __type_names = {}

    def __init__(self):

        ffi.cdef(self.__read_header())
        self.__lib = ffi.dlopen('libfdb5.dylib')

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

        # Initialise the library, and sett it up for python-appropriate behaviour

        self.fdc_initialise_api()

        # Check the library version

        tmp_str = ffi.new('char**')
        self.fdc_version(tmp_str)
        versionstr = ffi.string(tmp_str[0]).decode('utf-8')

        if parse_version(versionstr) < parse_version(__fdc_version__):
            raise RuntimeError("Version of libfdb found is too old. {} < {}".format(versionstr, __fdc_version__))

    #    def type_name(self, typ: 'DataType'):
    #        name = self.__type_names.get(typ, None)
    #        if name is not None:
    #            return name

    #        name_tmp = ffi.new('char**')
    #        self.odc_column_type_name(typ, name_tmp)
    #        name = ffi.string(name_tmp[0]).decode('utf-8')
    #        self.__type_names[typ] = name
    #        return name

    def __read_header(self):
        with open(os.path.join(os.path.dirname(__file__), 'processed_fdc.h'), 'r') as f:
            return f.read()

    def __check_error(self, fn, name):
        """
        If calls into the ODC library return errors, ensure that they get detected and reported
        by throwing an appropriate python exception.
        """

        def wrapped_fn(*args, **kwargs):
            retval = fn(*args, **kwargs)
            if retval != self.__lib.FDC_SUCCESS:
                error_str = "Error in function {}: {}".format(name, self.__lib.fdc_error_string(retval))
                raise FDCException(error_str)
            return retval

        return wrapped_fn


def memoize_constant(fn):
    """Memoize constant values to avoid repeatedly crossing the API layer unecessarily"""
    attr_name = "__memoized_{}".format(fn.__name__)

    def wrapped_fn(self):
        value = getattr(self, attr_name, None)
        if value is None:
            value = fn(self)
            setattr(self, attr_name, value)
        return value

    return wrapped_fn


# Bootstrap the library

lib = PatchedLib()


# Construct lookups/constants as is useful

# @unique
# class DataType(IntEnum):
#    IGNORE = lib.ODC_IGNORE
#    INTEGER = lib.ODC_INTEGER
#    DOUBLE = lib.ODC_DOUBLE
#    REAL = lib.ODC_REAL
#    STRING = lib.ODC_STRING
#    BITFIELD = lib.ODC_BITFIELD

# IGNORE = DataType.IGNORE
# INTEGER = DataType.INTEGER
# REAL = DataType.REAL
# STRING = DataType.STRING
# BITFIELD = DataType.BITFIELD
# DOUBLE = DataType.DOUBLE

class Key:
    __key = None

    def __init__(self):
        key = ffi.new('fdc_Key_t*')

        self.__key = ffi.gc(key, lib.fdc_Key_clean)


class KeySet:
    __keySet = None

    def __init__(self):
        keyset = ffi.new('fdc_KeySet_t*')

        self.__keySet = ffi.gc(keyset, lib.fdc_KeySet_clean)


class MarsRequest:
    __request = None

    def __init__(self, request_string):
        request = ffi.new('fdc_MarsRequest_t**')

        if not request_string:
            lib.fdc_MarsRequest_parse(request, "retrieve")
        else:
            lib.fdc_MarsRequest_parse(request, request_string)

        self.__request = ffi.gc(request[0], lib.fdc_MarsRequest_clean)


class ToolRequest:
    __request = None

    def __init__(self, all, keys, marsrequest):
        request = ffi.new('fdc_MarsRequest_t**')
        if all:
            lib.fdc_ToolRequest_init_all(request, keys)
        else:
            lib.fdc_ToolRequest_init(request, keys, marsrequest)

        self.__request = ffi.gc(request[0], lib.fdc_ToolRequest_clean)


class FDC:
    """This is the main container class for accessing FDB"""

    __fdc = None

    def __init__(self):

        fdc = ffi.new('fdc_t**')

        # Set free function
        self.__fdc = ffi.gc(fdc[0], lib.fdc_clean)

    @property
    def list(self, toolrequest):
        return ListIterator(self.__fdc, toolrequest)


class ListIterator:

    __iterator = None

    def __init__(self, fdb, request):
        iterator = ffi.new("fdc_ListIterator_t**")
        lib.fdc_list(fdb, request, iterator)

        self.__iterator = ffi.gc(iterator[0], lib.fdc_ListIterator_clean)

    def __iter__(self):
        exist = ffi.new("bool*")
        el = ffi.new("fdc_ListElement_t**")

        lib.fdc_list_next(self.__iterator, exist, el)
        if exist[0]:
            yield el

