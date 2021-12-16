from datetime import datetime

from chalicelib.aws.s3 import *

from smart_open import open


def test_list_subdirectories():
    prefix = "Florin/"
    result = list_subdirectories(prefix)
    assert result
    for subdirectory in result:
        assert subdirectory
        # assert subdirectory.index(prefix) < 0
    for subdir in list_subdirectories("Florin"):
        assert "Florin" == subdir


def test_cant_append_to_file():
    text = f"hello {datetime.now().toordinal()}"
    filepath = f"s3://{BUCKET}/Florin/test.txt"
    try:
        with open(filepath, "w") as g:
            g.seek(0, 2)
            g.write(text)
        assert False
    except:
        pass
    with open(filepath, "r") as f:
        assert text not in f.read()
