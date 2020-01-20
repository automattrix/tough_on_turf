import pandas as pd
import sqlite3
import os
import database

def connect_db():
    con = sqlite3.connect('./groupdb.sqlite')
    return con


def create_table():
    con = connect_db()
    cur = con.cursor()
    group_sql = """
    CREATE TABLE dirgroups (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        groupvalue integer,
        dirtype text)"""
    cur.execute(group_sql)
    con.close()

# TODO rename function and db table to reflect what is being written
def create_table_dirchange():
    con = connect_db()
    cur = con.cursor()
    group_sql = """
    CREATE TABLE dir_change (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dirvalue float,
        dirtype text,
        timesum float,
        num_values int,
        direction text,
        rel_ang_diff_start float,
        rel_ang_diff_end float,
        rel_ang_diff_change float,
        rel_ang_min float,
        rel_ang_max float,
        rel_ang_diff float)"""
    cur.execute(group_sql)
    con.commit()
    con.close()


def write_dir_change(dir_value, dirtype, timesum, num_values, direction, rel_ang_diff_start, rel_ang_diff_end,
                     rel_ang_change, rel_ang_min, rel_ang_max, rel_ang_diff):  # TODO make a generic sql write function
    insert_value = float(dir_value)
    con = connect_db()
    cur = con.cursor()

    parameters = (insert_value, dirtype, timesum, num_values, direction, rel_ang_diff_start, rel_ang_diff_end,
                  rel_ang_change, rel_ang_min, rel_ang_max, rel_ang_diff)
    cur.execute("INSERT OR IGNORE INTO dir_change VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", parameters)

    con.commit()
    con.close()


def write_current_group(group_value, direction_type):
    insert_value = int(group_value)
    con = connect_db()
    cur = con.cursor()
    parameters = (insert_value, direction_type)
    cur.execute("INSERT OR IGNORE INTO dirgroups VALUES (NULL, ?, ?)", parameters)

    con.commit()
    con.close()


def read_dirchange():
    con = connect_db()
    read_sql = "select id, dirvalue, dirtype, timesum, num_values, direction, rel_ang_diff_start, " \
               "rel_ang_diff_end, rel_ang_diff_change, rel_ang_min, rel_ang_max, rel_ang_diff " \
               "FROM dir_change " \
               "ORDER BY id asc;"
    current_group_df = pd.read_sql(sql=read_sql, con=con, index_col='id')

    return current_group_df


def read_current_group():
    con = connect_db()
    cur = con.cursor()
    read_sql = "select id, groupvalue, dirtype from dirgroups ORDER BY id desc LIMIT 1;"
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


def calc_groups(current_direction, next_direction, dfkey):
    tmp_group = read_current_group()

    # Get current group value from database
    current_group = int(tmp_group[1])

    # If player direction has not changed, use the same group value
    # Else, create new group value
    if current_direction == next_direction:
        return current_group
    else:
        next_group = current_group + 1
        write_current_group(group_value=next_group, direction_type=dfkey)
        return next_group


def calc_dir_change(groupdf, dfkey):
    # TODO rename function to reflect calculations inside -- not just direction changes
    df = groupdf

    group_direction = df['direction_shift'].iloc[0]
    time_sum = df['time_interval'].sum()
    num_measurements = len(df.index)

    start_dir = df[dfkey].iloc[0]
    end_dir = df[dfkey].iloc[-1]
    dir_change = abs(start_dir - end_dir)

    relative_angle_diff_start = df['head_v_body_diff'].iloc[0]
    relative_angle_diff_end = df['head_v_body_diff'].iloc[-1]
    relative_angle_diff_change = abs(relative_angle_diff_start - relative_angle_diff_end)

    relative_angle_min = df['head_v_body_diff'].min()
    relative_angle_max = df['head_v_body_diff'].max()
    relative_angle_diff = abs(relative_angle_max - relative_angle_min)  # Largest difference in angle between body, head

    write_dir_change(dir_value=dir_change, dirtype=dfkey, timesum=time_sum, num_values=num_measurements,
                     direction=group_direction, rel_ang_diff_start=relative_angle_diff_start,
                     rel_ang_diff_end=relative_angle_diff_end, rel_ang_change=relative_angle_diff_change,
                     rel_ang_min=relative_angle_min, rel_ang_max=relative_angle_max, rel_ang_diff=relative_angle_diff)


def calc_pct_of_max(dir_changes, maxdir, maxduration):
    # TODO rename function to reflect calculations inside -- not just percentages
    # Input if list of tuples - 0 is id, 1 is dir change

    # First calculate pct max for dir change
    # THen calculate pct of max for duration
    direction_dict = {}
    direction_dict.update({"max_dir_change": maxdir})
    direction_dict.update({"max_duration": maxduration})

    dir_changes['pct_change_max'] = (dir_changes['dirvalue'] / maxdir) * 100
    dir_changes['pct_duration_max'] = (dir_changes[
                                           'timesum'] / maxduration) * 100  # pct of max duration of direction change
    dir_changes['dir_ch_sec'] = dir_changes['dirvalue'] / dir_changes['timesum']  # how quickly player changed direction

    direction_dict.update({"data": dir_changes})

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
    #
    return angle_diff


def calc_rotation(df, dfkey):
    if os.path.exists('./groupdb.sqlite'):
        clear_db()
        write_current_group(group_value=0, direction_type=dfkey)
    else:
        # TODO make a db init function to create all the required tables
        create_table()
        create_table_dirchange()
        write_current_group(group_value=0, direction_type=dfkey)

    print(f"Calculating Rotation for {dfkey}")
    df = df.copy()

    # Calculate relative difference in degrees between head and body orientation
    df['head_v_body_diff'] = df[['o', 'dir']].apply(lambda x: calc_angle_diff(o=x['o'], dir=x['dir']), axis=1)

    # Calculate difference in orientation between measurements
    df["delta"] = df[dfkey].diff().fillna(0)

    # Calculate left of right change in direction
    df['pos_neg_orientation'] = df['delta'].apply(pos_neg_orientation)

    # Create new column shifted up by 1 row to compare current dir measurement to next dir measurement
    df['direction_shift'] = df['pos_neg_orientation'].shift(periods=-1, fill_value="no change")

    # Calculate groups ----------
    # A direction value is considered to be in the same group if the player direction has not changed
    df['groups'] = df[['pos_neg_orientation', 'direction_shift']].apply(
        lambda i: calc_groups(current_direction=i['pos_neg_orientation'], next_direction=i['direction_shift'], dfkey=dfkey), axis=1)

    # Calculate change in direction ---------
    unique_groups = df['groups'].unique()

    for group in unique_groups:
        group_df = df.loc[df['groups'] == group]
        # Calculate change in direction and angles, and write to database
        calc_dir_change(groupdf=group_df, dfkey=dfkey)

    dir_df = read_dirchange()

    # Find min and max dir change
    max_dir = dir_df['dirvalue'].max()
    max_duration = dir_df['timesum'].max()

    # Add additional columns, returns a dict, containing a DataFrame
    dir_dict = calc_pct_of_max(dir_changes=dir_df, maxdir=max_dir, maxduration=max_duration)
    return dir_dict
