import pandas as pd
import sqlite3
import os


# I'm so sorry I'm making another database function instead of making a generic database function like I should
# But here we are
# If you're reading this, I promise I'll clean up this code before I call this project complete


def connect_db():
    con = sqlite3.connect('./comparegroups.sqlite')
    return con


def create_tables():
    con = connect_db()
    cur = con.cursor()
    group_sql_d1 = """
    CREATE TABLE t_overlap_d1 (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        group_value integer,
        d1_dir text)"""
    cur.execute(group_sql_d1)
    con.commit()

    group_sql_d2 = """
        CREATE TABLE t_overlap_d2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_value integer,
            d2_dir text)"""
    cur.execute(group_sql_d2)
    con.commit()
    con.close()


def clear_db():
    con = connect_db()
    cur = con.cursor()
    delete_dirgroups = "DELETE FROM t_overlap_d1;"
    cur.execute(delete_dirgroups)
    con.commit()

    delete_overlap_increment = "delete from sqlite_sequence where name='t_overlap_d1';"
    cur.execute(delete_overlap_increment)
    con.commit()
    con.close()


def read_groupvalues(d):
    con = connect_db()
    read_sql = f"select id, group_value, d1_dir FROM t_overlap_{d} ORDER BY id asc;"
    current_group_val = pd.read_sql(sql=read_sql, con=con, index_col='id')
    print(current_group_val)

    #return current_group_val



def write_to_db(groupvalue, dir, num_values, d):
    #print(dir, num_values)
    current_value = 1

    con = connect_db()
    cur = con.cursor()

    while current_value < (num_values+1):
        print(current_value)
        current_group = groupvalue
        direction = dir

        parameters = (current_group, direction)
        print(parameters)
        cur.execute(f"INSERT OR IGNORE INTO t_overlap_{d} VALUES (NULL, ?, ?)", parameters)
        con.commit()
        current_value += 1
    read_groupvalues(d=d)



def compare_rotation(d1, d2):
    print("Comparing body and head rotation")
    # TODO sort by fastest direction change per sec
    # TODO sort by biggest change in direction
    # TODO sort by longest change in direction

    if os.path.exists('./comparegroups.sqlite'):
        clear_db()
    else:
        create_tables()

    # TODO create a function that determines whether or not part of a body/head dir group overlaps with another group
    # d1 head
    # d2 body

    # I need the ID as a column, so reset the index
    d1['data'].reset_index(inplace=True)
    d2['data'].reset_index(inplace=True)

    print(d1['data'].head())
    print(d2['data'].head())

    # Find number of values for each group where head and body were moving in the same direction

    d1['data'][['id', 'direction', 'num_values']].apply(lambda x: write_to_db(
        d='d1', groupvalue=x['id'], dir=x['direction'], num_values=x['num_values']), axis=1)


