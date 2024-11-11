from mylib/extract import extract
from transform_load import load

# from query import create
# from query import read
# from query import update
# from query import delete
from query import crud

if __name__ == "__main__":
    url = "https://raw.githubusercontent.com/nogibjj/ag825_sqlite_lab/refs/heads/main/Cancer_Data.csv"
    file_path = "Cancer_Data.csv"
    database = "CancerDB.db"

    # Extract
    print("Extracting data...")
    extract(url, file_path)

    # Transform and load
    print("Transforming data...")
    load()

    # Query
    print("Querying data...")
    crud()
    # read(database)
    # create(database)

    # update(database)
    # delete(database)