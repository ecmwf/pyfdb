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

        list_output = list(fdb.list(keys = True))
        assert len(list_output) == 1

        # Check that the archive path is in the tmp directory
        # On OSX tmp file paths look like /private/var/folders/.../T/tmp.../x138-300.grib
        # While the tmp directory looks like /var/folders/.../T/tmp.../ hence why this check is not "startwith"
        assert tmp_root in list_output[0]['path']

