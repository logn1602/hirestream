from importlib.metadata import version

import hirestream


def test_version_matches_installed_metadata() -> None:
    assert hirestream.__version__ == version("hirestream")
