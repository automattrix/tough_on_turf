import sqlite3


def connect_db(db_path):
    con = sqlite3.connect('./groupdb.sqlite')
    return con
