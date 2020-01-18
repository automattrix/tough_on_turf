import pandas as pd
import sqlite3
import os


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
        dirvalue float,
        timesum float,
        num_values int)"""
    cur.execute(group_sql)
    con.close()


def write_dir_change(dir_value, timesum, num_values):  # TODO make a generic sql write function
    insert_value = float(dir_value)
    con = connect_db()
    cur = con.cursor()

    write_sql = "insert or ignore into dir_change " \
                "(dirvalue, timesum, num_values) " \
                "values ({},{},{});".format(insert_value, timesum, num_values)
    cur.execute(write_sql)
    con.commit()
    con.close()


def write_current_group(group_value):
    insert_value = int(group_value)
    con = connect_db()
    cur = con.cursor()

    write_sql = "insert or ignore into dirgroups " \
                "(groupvalue) " \
                "values ({});".format(insert_value)
    cur.execute(write_sql)
    con.commit()
    con.close()


def read_dirchange():
    con = connect_db()
    cur = con.cursor()
    read_sql = "select id, dirvalue, timesum, num_values from dir_change ORDER BY id desc;"
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

    write_dir_change(dir_value=dir_change, timesum=time_sum, num_values=num_measurements)
    #print(group_direction, start_dir, end_dir, dir_change)


# TODO rename function to reflect calculations inside -- not just percentages
def calc_pct_of_max(dir_changes, maxdir, maxduration):
    # Input if list of tuples - 0 is id, 1 is dir change

    # First calculate pct max for dir change
    # THen calculate pct of max for duration
    direction_dict = {}
    direction_dict.update({"max_dir_change": maxdir})
    direction_dict.update({"max_duration": maxduration})

    direction_dict.update({"groups": {}})
    for i in dir_changes:
        dirgroup = i[0]
        dirvalue = i[1]
        durationvalue = i[2]

        pct_change_max = (dirvalue / maxdir) * 100  # pct of max dir change
        pct_duration_max = (durationvalue / maxduration) * 100  # pct of max duration of direction change
        dir_ch_sec = dirvalue / durationvalue  # how quickly player changed direction
        direction_dict['groups'].update({dirgroup: {}})
        direction_dict['groups'][dirgroup].update({"pct_ch_max": pct_change_max})
        direction_dict['groups'][dirgroup].update({"pct_dur_max": pct_duration_max})
        direction_dict['groups'][dirgroup].update({"dir_ch_per_sec": dir_ch_sec})

    return direction_dict


def pos_neg_orientation(orientation):

    if orientation > 0:
        return "right"
    elif orientation < 0:
        return "left"
    else:
        return "no change"


def calc_angle_diff(o, dir):
    o_diff = (360 - o)
    dir_diff = (360 - dir)
    abs_diff = abs(o_diff - dir_diff)
    tmp_angle_diff = 360 - abs_diff

    if tmp_angle_diff > 180:
        angle_diff = 360 - tmp_angle_diff
    else:
        angle_diff = tmp_angle_diff

    return angle_diff


def calc_rotation(df, dfkey):
    if os.path.exists('./groupdb.sqlite'):
        clear_db()
        write_current_group(group_value=0)
    else:
        # TODO make a db init function to create all the required tables
        create_table()
        create_table_dirchange()
        write_current_group(group_value=0)

    print(f"Calculating Rotation for {dfkey}")
    df = df.copy()

    # Calculate relative difference in degrees between head and body orientation
    df['head_v_body_diff'] = df[['o', 'dir']].apply(lambda x: calc_angle_diff(o=x['o'], dir=x['dir']), axis=1)

    # Calculate difference in orientation between measurements
    df["delta"] = df[f'{dfkey}'].diff().fillna(0)

    # Calculate left of right change in direction
    df['pos_neg_orientation'] = df['delta'].apply(pos_neg_orientation)

    # Create new column shifted up by 1 row to compare current dir measurement to next dir measurement
    df['direction_shift'] = df['pos_neg_orientation'].shift(periods=-1, fill_value="no change")

    # Calculate groups ----------
    # A direction value is considered to be in the same group if the player direction has not changed
    df['groups'] = df[['pos_neg_orientation', 'direction_shift']].apply(
        lambda i: calc_groups(current_direction=i['pos_neg_orientation'], next_direction=i['direction_shift']), axis=1)

    # Calculate change in direction ---------
    unique_groups = df['groups'].unique()

    # Create empty dict for storing dataframes
    dir_dict_tmp = {}
    # Create dataframe for each group and store in dict
    for group in unique_groups:
        group_df = df.loc[df['groups'] == group]
        dir_dict_tmp.update({group: group_df})
        calc_dir_change(groupdf=group_df)

    # Calculate min and max dir change
    # Sorted by dir change
    dir_change_sort = sorted([i for i in read_dirchange()], key=lambda x: x[1])  # Sorted by biggest change in direction
    min_dir = float(dir_change_sort[0][1])
    max_dir = float(dir_change_sort[-1][1])

    dir_duration_sort = sorted(dir_change_sort, key=lambda y: y[2])  # Sorted by duration of direction change
    min_duration = float(dir_duration_sort[0][2])
    max_duration = float(dir_duration_sort[-1][2])

    #print(min_dir, max_dir, min_duration, max_duration)

    dir_dict = calc_pct_of_max(dir_changes=dir_change_sort, maxdir=max_dir, maxduration=max_duration)
    return dir_dict
