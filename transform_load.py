import sqlite3
import csv
import os


def load():
    dataset = "Cancer_Data.csv"
    encoding = "utf-8"
    # prints the full working directory and path
    print(os.getcwd())
    payload = csv.reader(open(dataset, newline="", encoding=encoding), delimiter=",")
    conn = sqlite3.connect("CancerDB.db")
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS CancerDB")
    c.execute(
        """CREATE TABLE CancerDB (id,diagnosis,radius_mean,
        texture_mean,perimeter_mean,area_mean,smoothness_mean)"""
    )
    # insert
    c.executemany("INSERT INTO CancerDB VALUES (?, ?, ?, ?, ?, ?, ?)", payload)
    conn.commit()
    conn.close()
    return "CancerDB.db"


if __name__ == "__main__":
    load()
