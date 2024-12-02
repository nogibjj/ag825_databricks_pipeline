"""Query the database"""

import sqlite3


def create(database):
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO CancerDB (id,diagnosis,radius_mean,texture_mean,
        perimeter_mean,area_mean,smoothness_mean) 
        VALUES ('123123123', 'M', 12.3123312, 21.312312312, 
        12.3213123, 21.3123213, 23.1231312) 
                   """
    )
    conn.commit()
    cursor.execute("SELECT * FROM CancerDB WHERE id='123123123' LIMIT 5")
    print("The following record has been inserted into the database:")
    print(cursor.fetchall())
    conn.close()
    return "Success"


def read(database):
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM CancerDB LIMIT 5")
    print("Top 5 rows of the CancerDB table:")
    print(cursor.fetchall())
    conn.close()
    return "Success"


def update(database):
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    cursor.execute(
        """ UPDATE CancerDB
            SET radius_mean='32.234234423'
            WHERE id='123123123';
            """
    )
    print("The following record has been updated in the database:")
    cursor.execute("SELECT * FROM CancerDB WHERE id='123123123' LIMIT 5")
    print(cursor.fetchall())
    conn.close()
    return "Success"


def delete(database):
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    cursor.execute(
        """ DELETE FROM CancerDB
                   WHERE id = '123123123'
                   """
    )
    conn.commit()
    print("The record has been deelted from the database (no output will be seen here)")
    cursor.execute("SELECT * FROM CancerDB WHERE id='123123123' LIMIT 5")
    print(cursor.fetchall())
    conn.close()
    return "Success"


def crud():
    database = "CancerDB.db"
    read(database)
    create(database)

    update(database)
    delete(database)


if __name__ == "__main__":
    crud()
