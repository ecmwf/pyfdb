from pytest import raises

import pyfdb


def test_direct_config_2():
    # pyfdb uses findlibs internally to locate the library
    # So this looks a bit circular but it's just to check that we can pass a path explicitly
    # The `libpath` argument allows the user to hard code the path to the library.
    # This should not be necessary in normal usage but it's a useful backup.
    lib_path = "made up path"
    assert raises(FileNotFoundError, pyfdb.FDB, lib_path=lib_path)
    assert pyfdb.lib.path == lib_path
