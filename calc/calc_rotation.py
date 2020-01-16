import pandas as pd
import sqlite3
import os
import datetime
import time


def connect_db():
    con = sqlite3.connect('./groupdb.sqlite')
    return con


def create_table():
    con = connect_db()
    cur = con.cursor()
    group_sql = """
    CREATE TABLE dirgroups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        groupvalue integer)"""
    cur.execute(group_sql)
    con.close()


def create_table_dirchange():
    con = connect_db()
    cur = con.cursor()
    group_sql = """
    CREATE TABLE dir_change (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dirvalue float)"""
    cur.execute(group_sql)
    con.close()


def write_dir_change(dir_value): # TODO make a generic sql write function
    insert_value = float(dir_value)
    con = connect_db()
    cur = con.cursor()

    write_sql = "insert or ignore into dir_change (dirvalue) values ({});".format(insert_value)
    cur.execute(write_sql)
    con.commit()
    con.close()


def write_current_group(group_value):
    insert_value = int(group_value)
    con = connect_db()
    cur = con.cursor()
    epoch = str(time.time())

    write_sql = "insert or ignore into dirgroups (groupvalue) values ({});".format(insert_value)
    cur.execute(write_sql)
    con.commit()
    con.close()


def read_dirchange():
    con = connect_db()
    cur = con.cursor()
    read_sql = "select id, dirvalue from dir_change ORDER BY id desc;"
    cur.execute(read_sql)
    current_group = cur.fetchall()
    # print(current_group)
    return current_group


def read_current_group():
    con = connect_db()
    cur = con.cursor()
    read_sql = "select id, groupvalue from dirgroups ORDER BY id desc LIMIT 1;"
    cur.execute(read_sql)
    current_group = cur.fetchone()
    # print(current_group)
    return current_group


def clear_db():
    con = connect_db()
    cur = con.cursor()
    delete_dirgroups = "DELETE FROM dirgroups;"
    cur.execute(delete_dirgroups)
    con.commit()

    delete_dirchange = "DELETE FROM dir_change;"
    cur.execute(delete_dirchange)
    con.commit()

    delete_dirgroup_increment = "delete from sqlite_sequence where name='dirgroup';"
    delete_dirchange_increment = "delete from sqlite_sequence where name='dir_change';"
    cur.execute(delete_dirgroup_increment)
    cur.execute(delete_dirchange_increment)
    con.commit()
    con.close()


def calc_groups(current_direction, next_direction):
    tmp_group = read_current_group()
    #print(tmp_group)

    # Get current group value from database
    current_group = int(tmp_group[1])

    # If player direction has not changed, use the same group value
    # Else, create new group value
    if current_direction == next_direction:
        return current_group
    else:
        next_group = current_group + 1
        write_current_group(group_value=next_group)
        return next_group


def calc_dir_change(groupdf):
    df = groupdf

    group_direction = df['direction_shift'].iloc[0]
    time_sum = df['time_interval'].sum()
    num_measurements = len(df.index)

    start_dir = df['o'].iloc[0]
    end_dir = df['o'].iloc[-1]
    dir_change = abs(start_dir - end_dir)

    write_dir_change(dir_value=dir_change)
    # TODO pickup here, just calculated dir_change per group
    print(group_direction, start_dir, end_dir, dir_change)


def pos_neg_orientation(orientation):

    if orientation > 0:
        return "right"
    elif orientation < 0:
        return "left"
    else:
        return "no change"


def calc_rotation(df):
    if os.path.exists('./groupdb.sqlite'):
        clear_db()
        write_current_group(group_value=0)
    else:
        # TODO make a db init function to create all the required tables
        create_table()
        create_table_dirchange()
        write_current_group(group_value=0)

    print("Calculating Rotation")
    print(df['o'].head())
    df = df.copy()
    # Calculate difference in orientation between measurements
    df["o_delta"] = df['o'].diff().fillna(0)
    print(df['o_delta'])

    # Calculate left of right change in direction
    df['pos_neg_orientation'] = df['o_delta'].apply(pos_neg_orientation)
    print(df['pos_neg_orientation'].head())

    # Create new column shifted up by 1 row to compare current dir measurement to next dir measurement
    df['direction_shift'] = df['pos_neg_orientation'].shift(periods=-1, fill_value="no change")
    print(df[['pos_neg_orientation', 'direction_shift']].head())

    # Calculate groups ----------
    # A direction value is considered to be in the same group if the player direction has not changed
    df['groups'] = df[['pos_neg_orientation','direction_shift']].apply(
        lambda i: calc_groups(current_direction=i['pos_neg_orientation'], next_direction=i['direction_shift']), axis=1)

    # Calculate change in direction ---------
    unique_groups = df['groups'].unique()

    # Create dataframe for each group
    for group in unique_groups:
        group_df = df.loc[df['groups'] == group]
        calc_dir_change(groupdf=group_df)

    # Calculate min and max dir change
    #test = read_dirchange()
    #print(test)
    #exit()
    dir_changes = sorted([i for i in read_dirchange()], key=lambda x: x[1])
    min_dir = dir_changes[0]
    max_dir = dir_changes[-1]
    print(dir_changes)
    print(min_dir, max_dir)

    # Print summary
    #print((df[['pos_neg_orientation','direction_shift', 'groups', 'o']].head()))
