import pytest

from jobmon.client.units import MemUnit, TimeUnit
from jobmon.core.exceptions import InvalidMemoryFormat, InvalidMemoryUnit

tu_test_data = [
    (1, 0, 0, (1, 0.02, 0.0, "0:00:01")),
    (0, 1, 0, (60, 1.0, 0.02, "0:01:00")),
    (0, 0, 1, (3600, 60.0, 1.0, "1:00:00")),
    (10, 1, 1.5, (5470, 91.17, 1.52, "1:31:10")),
    (7, 5.25, 24, (86722, 1445.37, 24.09, "1 day, 0:05:22")),
]


@pytest.mark.parametrize("s, m, h, e", tu_test_data)
def test_timeunit_init(s, m, h, e):
    tu = TimeUnit(sec=s, min=m, hour=h)
    assert tu.seconds == e[0]
    assert tu.minutes == e[1]
    assert tu.hours == e[2]
    assert tu.readable == e[3]


mu_test_data = [
    ("1G", "Mb", 1024),
    ("1g", "M", 1024),
    ("1g", "m", 1024),
    ("1gb", "m", 1024),
    ("1Gib", "M", 1024),
    ("2Tb", "Gib", 2048),
    ("2048K", "M", 2),
    ("2048kB", "M", 2),
    ("100mb", "M", 100),
    ("1024kb", "M", 1),
    ("1gib", "G", 1),
    (100, "M", 100),
]


@pytest.mark.parametrize("i, t, e", mu_test_data)
def test_memunit_convert(i, t, e):
    assert MemUnit.convert(i, t) == e


@pytest.mark.parametrize("input", ["a", "10 M", "1g1M"])
def test_memunit_wrong_input(input):
    with pytest.raises(InvalidMemoryFormat):
        MemUnit.convert(input)


@pytest.mark.parametrize("to", ["a", "gg", "1g1M"])
def test_memunit_unit(to):
    with pytest.raises(InvalidMemoryUnit):
        MemUnit.convert(100, to)
