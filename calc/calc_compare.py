import pandas as pd
import sqlite3
import os


# I'm so sorry I'm making another database function instead of making a generic database function like I should
# But here we are
# If you're reading this, I promise I'll clean up this code before I call this project complete


def connect_db():
    con = sqlite3.connect('./comparegroups.sqlite')
    return con


def create_table():
    con = connect_db()
    cur = con.cursor()
    group_sql = """
    CREATE TABLE t_overlap (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        d1 dir text,
        d2 dir text)"""
    cur.execute(group_sql)
    con.close()


def clear_db():
    con = connect_db()
    cur = con.cursor()
    delete_dirgroups = "DELETE FROM t_overlap;"
    cur.execute(delete_dirgroups)
    con.commit()

    delete_overlap_increment = "delete from sqlite_sequence where name='t_overlap';"
    cur.execute(delete_overlap_increment)
    con.commit()
    con.close()


def overlapping_dir(d1_dir, d1_num, d2_dir, d2_num):
    print("overlap")


def compare_rotation(d1, d2):
    print("Comparing body and head rotation")
    # TODO sort by fastest direction change per sec
    # TODO sort by biggest change in direction
    # TODO sort by longest change in direction

    if os.path.exists('./comparegroups.sqlite'):
        clear_db()
    else:
        create_table()

    # TODO create a function that determines whether or not part of a body/head dir group overlaps with another group
    # d1 head
    # d2 body

    #print(d1.keys())
    #print(d1['groups'])
    print(d1['data'].keys())
    print(len(d1['data'].keys()))
    print(len(d1['data'].index))

    print(d1['data'].head())
    print(d2['data'].head())
    #d1_order_group = sorted(d1, key=lambda x: x)

    # Find number of values for each group where head and body were moving in the same direction

    d1['same_dir'] = ''
    d1['differnet_dir'] = ''

    d2['same_dir'] = ''
    d2['differnet_dir'] = ''