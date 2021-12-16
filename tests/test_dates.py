from datetime import datetime
from chalicelib.dates import *


def test_parse_s3_date():
    assert parse_s3_date("20201010_1010") == datetime(2020, 10, 10, 10, 10)
    pass


def test_unparse_s3_date():
    assert unparse_s3_date(datetime(2021, 11, 21, 21, 21).astimezone()) == "20211121_2121"
    pass
