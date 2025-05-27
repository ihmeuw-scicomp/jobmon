import getpass
import os
import os.path as path

import pytest

from jobmon.core.jobmon_utils import true_path


def test_true_path():
    with pytest.raises(ValueError) as exc_info:
        true_path()
    assert "cannot both" in str(exc_info.value)

    assert true_path("") == os.getcwd()
    assert getpass.getuser() in true_path("~/bin")
    assert true_path("blah").endswith("/blah")
    assert true_path(file_or_dir=".") == path.abspath(".")
    # the path differs based on the cluster but all are in /bin/time
    # (some are in /usr/bin/time)
    assert "/bin/time" in true_path(executable="time")
