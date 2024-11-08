import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

from pytest import raises

import pyfdb


def test_conflicting_arguments():
    "You can't pass both fdb_home and config because the former overrides the latter"
    assert raises(ValueError, pyfdb.FDB, fdb_home="/tmp", config={})


def test_fdb_home():
    with TemporaryDirectory() as tmp_home:
        tests_dir = Path(__file__).parent
        schema_path = tests_dir / "default_fdb_schema"

        home = Path(tmp_home)
        etc = home / "etc" / "fdb"
        root_path = home / "root"
        root_path.mkdir(parents=True)
        etc.mkdir(parents=True)

        shutil.copy(schema_path, etc / "schema")

        with open(etc / "config.yaml", "w") as f:
            f.write(
                f"""
---
type: local
engine: toc
schema: {etc / "schema"}
spaces:
- handler: Default
  roots:
    - path: "{root_path}"
      writable: true
      visit: true
"""
            )

        with open(etc / "config.yaml", "r") as f:
            print(f.read())

        fdb = pyfdb.FDB(fdb_home=home)
        data = open(tests_dir / "x138-300.grib", "rb").read()
        fdb.archive(data)
        fdb.flush()

        list_output = list(fdb.list(keys=True))
        assert len(list_output) == 1

        # Check that the archive path is in the tmp directory
        # On OSX tmp file paths look like /private/var/folders/.../T/tmp.../x138-300.grib
        # While the tmp directory looks like /var/folders/.../T/tmp.../ hence why this check is not "startwith"

        # Disabled for now because the HPC has a different convention and I don't know how to deal with all cases
        # On the HPC
        # tmp_home looks like '/etc/ecmwf/ssd/ssd1/tmpdirs/***.31103395/tmpg137a4ml'
        # while the archive path looks like '/etc/ecmwf/ssd/ssd1/tmpdirs/***.31103395/data/fdb/...'

        # assert str(Path(tmp_home).resolve()) in str(Path(list_output[0]["path"]).resolve())


def test_direct_config():
    with TemporaryDirectory() as tmp_root:
        tests_dir = Path(__file__).parent

        config = dict(
            type="local",
            engine="toc",
            schema=str(tests_dir / "default_fdb_schema"),
            spaces=[
                dict(
                    handler="Default",
                    roots=[
                        {"path": tmp_root},
                    ],
                )
            ],
        )

        fdb = pyfdb.FDB(config)
        data = open(tests_dir / "x138-300.grib", "rb").read()
        fdb.archive(data)
        fdb.flush()

        list_output = list(fdb.list(keys=True))
        assert len(list_output) == 1

        # Check that the archive path is in the tmp directory
        # On OSX tmp file paths look like /private/var/folders/.../T/tmp.../x138-300.grib
        # While the tmp directory looks like /var/folders/.../T/tmp.../ hence why this check is not "startwith"

        # Disabled for now because the HPC has a different convention and I don't know how to deal with all cases
        # assert tmp_root in list_output[0]["path"]


def test_opening_two_fdbs():
    with TemporaryDirectory() as tmp_root1, TemporaryDirectory() as tmp_root2:
        tests_dir = Path(__file__).parent

        fdb1 = pyfdb.FDB(
            dict(
                type="local",
                engine="toc",
                schema=str(tests_dir / "default_fdb_schema"),
                spaces=[
                    dict(
                        handler="Default",
                        roots=[
                            {"path": tmp_root1},
                        ],
                    )
                ],
            )
        )

        fdb2 = pyfdb.FDB(
            dict(
                type="local",
                engine="toc",
                schema=str(tests_dir / "default_fdb_schema"),
                spaces=[
                    dict(
                        handler="Default",
                        roots=[
                            {"path": tmp_root2},
                        ],
                    )
                ],
            )
        )

        for fdb in [fdb1, fdb2]:
            data = open(tests_dir / "x138-300.grib", "rb").read()
            fdb.archive(data)
            fdb.flush()

        # Disabled for now
        # for fdb, root in [(fdb1, tmp_root1), (fdb2, tmp_root2)]:
        #     list_output = list(fdb.list(keys=True))
        #     assert len(list_output) == 1
        #     assert root in list_output[0]["path"]
