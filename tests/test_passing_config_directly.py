from pathlib import Path
from tempfile import TemporaryDirectory

import pyfdb


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
        assert tmp_root in list_output[0]["path"]


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

        for fdb, root in [(fdb1, tmp_root1), (fdb2, tmp_root2)]:
            list_output = list(fdb.list(keys=True))
            assert len(list_output) == 1
            assert root in list_output[0]["path"]
