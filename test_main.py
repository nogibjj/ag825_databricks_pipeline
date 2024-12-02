from query import create
from query import read
from query import update
from query import delete


def test_create():
    database = "CancerDB.db"
    assert create(database) == "Success"


def test_read():
    database = "CancerDB.db"
    assert read(database) == "Success"


def test_update():
    database = "CancerDB.db"
    assert update(database) == "Success"


def test_delete():
    database = "CancerDB.db"
    assert delete(database) == "Success"


if __name__ == "__main__":

    database = "CancerDB.db"

    test_create()
    test_read()
    test_update()
    test_delete()
