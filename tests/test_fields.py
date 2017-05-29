import pytest

from api_li3ds.fields import DateTime


@pytest.fixture
def datetime():
    datetime = DateTime(dt_format='iso8601')
    return datetime


def test_DateTime(datetime):
    assert datetime.format('2011-10-05T17:31:16.32+02') == \
            '2011-10-05T15:31:16.320000+00:00'
