import pandas as pd
import sqlite3
import os
import re

# start here


def create_bodypart_df_list(csv_list):
    df_list = [pd.read_csv(csv_path) for csv_path in csv_list]
    return df_list


class Player:
    def __init__(self, csv_path):
        self.player_data_path = csv_path
        self.player_data = self.load_df()

    def load_df(self):
        df = pd.read_csv(self.player_data_path)
        return df

    def playerkeys(self):
        print("Calculating playkeys")
        unique_playkeys = self.player_data['PlayKey'].unique()
        return unique_playkeys


#  CALC SPEED ------------------------------------------------------------------------
def calc_speed(df):
    unique_velocities = df['pos_neg_vel'].unique()
    avg_total_vel = df['velocity'].mean()

    tmp_dict = {}

    tmp_dict.update({"total_avg": avg_total_vel})
    for vel in unique_velocities:
        tmp_dict.update({vel: {}})
        # Create sub df
        df_vel = df.loc[df['pos_neg_vel'] == vel]

        # Average of velocity
        avg_vel = df_vel['velocity'].mean()
        tmp_dict[vel].update({"avg_vel": avg_vel})

        # Average of change in velocity
        avg_vel_change = df_vel['velocity_change'].mean()
        tmp_dict[vel].update({"avg_vel_change": avg_vel_change})

        # Max velocity
        max_velocity = df_vel['velocity'].max()
        tmp_dict[vel].update({"max_vel": max_velocity})

        # Max velocity Change
        max_velocity_change = df_vel['velocity_change'].max()
        tmp_dict[vel].update({"max_vel_change": max_velocity_change})

    return tmp_dict
#  CALC SPEED END ------------------------------------------------------------------------

#  CALC GROUPMETRICS START ------------------------------------------------------------------------


def connect_db_group():
    con = sqlite3.connect('./groupdb.sqlite')
    return con


def create_table_group():
    con = connect_db_group()
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
    con = connect_db_group()
    cur = con.cursor()
    group_sql = """
        CREATE TABLE group_metrics (
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
        rel_ang_diff float,
        vel_avg float,
        vel_avg_change float,
        vel_min float,
        vel_max float,
        vel_change_min float,
        vel_change_max float
        )"""
    cur.execute(group_sql)
    con.commit()
    con.close()


def write_dir_change(dir_value, dirtype, timesum, num_values, direction, rel_ang_diff_start, rel_ang_diff_end,
                     rel_ang_change, rel_ang_min, rel_ang_max, rel_ang_diff, vel_avg, vel_avg_change, vel_min, vel_max,
                     vel_change_min, vel_change_max):  # TODO make a generic sql write function UGH DO IT ALREADY
    insert_value = float(dir_value)
    con = connect_db_group()
    cur = con.cursor()

    parameters = (insert_value, dirtype, timesum, num_values, direction, rel_ang_diff_start, rel_ang_diff_end,
                  rel_ang_change, rel_ang_min, rel_ang_max, rel_ang_diff, vel_avg, vel_avg_change, vel_min, vel_max,
                  vel_change_min, vel_change_max)
    cur.execute("INSERT OR IGNORE "
                "INTO group_metrics "
                "VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", parameters)

    con.commit()
    con.close()


def write_current_group(group_value, direction_type):
    insert_value = int(group_value)
    con = connect_db_group()
    cur = con.cursor()
    parameters = (insert_value, direction_type)
    cur.execute("INSERT OR IGNORE INTO dirgroups VALUES (NULL, ?, ?)", parameters)

    con.commit()
    con.close()


def read_dirchange():
    con = connect_db_group()
    read_sql = "select id, dirvalue, dirtype, timesum, num_values, direction, rel_ang_diff_start, " \
               "rel_ang_diff_end, rel_ang_diff_change, rel_ang_min, rel_ang_max, rel_ang_diff, vel_avg, " \
               "vel_avg_change, vel_min, vel_max, vel_change_min, vel_change_max " \
               "FROM group_metrics " \
               "ORDER BY id asc;"
    current_group_df = pd.read_sql(sql=read_sql, con=con, index_col='id')

    return current_group_df


def read_current_group():
    con = connect_db_group()
    cur = con.cursor()
    read_sql = "select id, groupvalue, dirtype from dirgroups ORDER BY id desc LIMIT 1;"
    cur.execute(read_sql)
    current_group = cur.fetchone()
    # print(current_group)
    return current_group


def clear_db_group():
    con = connect_db_group()
    cur = con.cursor()
    delete_dirgroups = "DELETE FROM dirgroups;"
    cur.execute(delete_dirgroups)
    con.commit()

    delete_dirchange = "DELETE FROM group_metrics;"
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
    #print(df.keys())
    group_direction = df['direction_shift'].iloc[0]
    time_sum = df['time_interval'].sum()
    num_measurements = len(df.index)

    # Calculate change in direction
    start_dir = df[dfkey].iloc[0]
    end_dir = df[dfkey].iloc[-1]
    dir_change = abs(start_dir - end_dir)

    # Calculate change in relative angle between head and body
    relative_angle_diff_start = df['head_v_body_diff'].iloc[0]
    relative_angle_diff_end = df['head_v_body_diff'].iloc[-1]
    relative_angle_diff_change = (relative_angle_diff_end - relative_angle_diff_start)

    # Calculate difference between min and max relative angle -- note head & body don't always line up
    relative_angle_min = df['head_v_body_diff'].min()
    relative_angle_max = df['head_v_body_diff'].max()
    relative_angle_diff = abs(relative_angle_max - relative_angle_min)  # Largest difference in angle between body, head

    # Calculate velocity
    vel_avg = df['velocity'].mean()
    vel_avg_change = df['velocity_change'].mean()

    vel_min = df['velocity'].min()
    vel_max = df['velocity'].max()

    vel_change_min = df['velocity_change'].min()
    vel_change_max = df['velocity_change'].max()

    write_dir_change(dir_value=dir_change, dirtype=dfkey, timesum=time_sum, num_values=num_measurements,
                     direction=group_direction, rel_ang_diff_start=relative_angle_diff_start,
                     rel_ang_diff_end=relative_angle_diff_end, rel_ang_change=relative_angle_diff_change,
                     rel_ang_min=relative_angle_min, rel_ang_max=relative_angle_max, rel_ang_diff=relative_angle_diff,
                     vel_avg=vel_avg, vel_avg_change=vel_avg_change, vel_min=vel_min, vel_max=vel_max,
                     vel_change_min=vel_change_min, vel_change_max=vel_change_max)


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


def iterate_player(df):
    for player_csv in df.itertuples():
        player_df = pd.read_csv(player_csv[1])
        yield player_df


def _player_csv_dict(df_csv_list):
    bodypart_dict = {}
    for csv_path in df_csv_list:
        df = pd.read_csv(csv_path)
        body_part = re.search('(?<=./data/02_intermediate//)(\w+)', csv_path).group(0)
        bodypart_dict[body_part] = df

    return bodypart_dict


def calc_metrics(df_csv_list, params):

    print(df_csv_list)
    # bodypart_list: df
    df_list = _player_csv_dict(df_csv_list=df_csv_list)

    dir_dicts_list = []

    for bodypart, df in df_list.items():
        for x in iterate_player(df=df):

        for dfkey in params['df_keys']:

            if os.path.exists(params['db_path']):
                clear_db_group()
                write_current_group(group_value=0, direction_type=dfkey)
            else:
                # TODO make a db init function to create all the required tables
                create_table_group()
                create_table_dirchange()
                write_current_group(group_value=0, direction_type=dfkey)

            print(f"Calculating Rotation for {dfkey}")
            df = df.copy()
            print(df.head())
            exit()
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
            dir_dicts_list.append({bodypart: dir_dict})
    print(dir_dicts_list)
    exit()
    return dir_dicts_list
#  CALC GROUPMETRICS END ------------------------------------------------------------------------


#  CALC COMPARE START ------------------------------------------------------------------------

# I'm so sorry I'm making another database function instead of making a generic database function like I should
# But here we are
# If you're reading this, I promise I'll clean up this code before I call this project complete


def connect_db_compare(params):
    con = sqlite3.connect(params['db_path'])
    return con


def create_tables_compare():
    con = connect_db_compare()
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


def clear_db_compare():
    con = connect_db_compare()
    cur = con.cursor()
    delete_dirgroups_d1 = "DELETE FROM t_overlap_d1;"
    delete_dirgroups_d2 = "DELETE FROM t_overlap_d2;"
    cur.execute(delete_dirgroups_d1)
    cur.execute(delete_dirgroups_d2)
    con.commit()

    delete_overlap_increment_d1 = "delete from sqlite_sequence where name='t_overlap_d1';"
    delete_overlap_increment_d2 = "delete from sqlite_sequence where name='t_overlap_d2';"  # I'm sorry..I know..
    cur.execute(delete_overlap_increment_d1)
    cur.execute(delete_overlap_increment_d2)
    con.commit()
    con.close()


def read_groupvalues(d):
    con = connect_db_compare()
    read_sql = f"select id, group_value, d1_dir FROM t_overlap_{d} ORDER BY id asc;"
    current_group_val = pd.read_sql(sql=read_sql, con=con, index_col='id')
    print(current_group_val)

    #return current_group_val


def compare_joined_values(a, b):
    if a == b:
        return "same"
    else:
        return "opposite"


def read_joined_values(d, group):
    if d == 'd1':
        otherkey = 'd2'
    elif d == 'd2':
        otherkey = 'd1'
    else:
        exit("Invalid Key")

    con = connect_db_compare()

    read_sql = f"select a.id as id, a.group_value, a.{d}_dir, b.group_value, b.{otherkey}_dir FROM t_overlap_{d} a " \
        f"JOIN t_overlap_{otherkey} b ON a.id = b.id WHERE a.group_value = {group} ORDER BY a.id asc;"

    joined_values_df = pd.read_sql(sql=read_sql, con=con, index_col='id')

    joined_values_df['compare'] = joined_values_df[['d1_dir', 'd2_dir']].apply(
        lambda x: compare_joined_values(a=x[f'{d}_dir'], b=x[f'{otherkey}_dir']), axis=1)

    same_df = joined_values_df.loc[joined_values_df['compare'] == 'same']
    overlap_dir = len(same_df.index)

    return overlap_dir


def write_to_db(groupvalue, dir, num_values, d):
    current_value = 1

    con = connect_db_compare()
    cur = con.cursor()

    while current_value < (num_values+1):
        current_group = groupvalue
        direction = dir

        parameters = (current_group, direction)
        cur.execute(f"INSERT OR IGNORE INTO t_overlap_{d} VALUES (NULL, ?, ?)", parameters)
        con.commit()
        current_value += 1


def compare_rotation(df_list, params):

    print("Comparing body and head rotation")
    # TODO sort by fastest direction change per sec
    # TODO sort by biggest change in direction
    # TODO sort by longest change in direction

    if os.path.exists(params['db_path']):
        clear_db_compare()
    else:
        create_tables_compare()

    # TODO create a function that determines whether or not part of a body/head dir group overlaps with another group
    # d1 head
    d1 = df_list[0]
    # d2 body
    d2 = df_list[1]

    # I need the ID as a column, so reset the index
    d1['data'].reset_index(inplace=True)
    d2['data'].reset_index(inplace=True)

    #print(d1['data'].head())
    #print(d2['data'].head())
    # Find number of values for each group where head and body were moving in the same direction
    # First write values to dtabase for each group
    d1['data'][['id', 'direction', 'num_values']].apply(lambda x: write_to_db(
        d='d1', groupvalue=x['id'], dir=x['direction'], num_values=x['num_values']), axis=1)

    d2['data'][['id', 'direction', 'num_values']].apply(lambda x: write_to_db(
        d='d2', groupvalue=x['id'], dir=x['direction'], num_values=x['num_values']), axis=1)

    # Now read back the values and compare
    # d1 head orientation
    d1['data']['overlap'] = d1['data'][['id']].apply(lambda x: read_joined_values(d='d1', group=x['id']), axis=1)
    d1['data']['overlap_pct'] = (d1['data']['overlap'] / d1['data']['num_values']) * 100
    #print(d1['data'].head(50))

    # d2 body orientation
    # Leaning towards d2 (body) orientation being weighted more than d1, as the body generates more momentum
    d2['data']['overlap'] = d2['data'][['id']].apply(lambda x: read_joined_values(d='d2', group=x['id']), axis=1)
    d2['data']['overlap_pct'] = (d2['data']['overlap'] / d2['data']['num_values']) * 100
    #print(d2['data'].head(50))

    # Temporary output to csv
    #d2['data'].to_csv('./test_compare_d2.csv')
    return d1, d2
#  CALC COMPARE END ------------------------------------------------------------------------


#  CALC RISK START ------------------------------------------------------------------------
def score(df_list):
    # Temporarily manually specifying the 'dir' df, instead of orientation df
    # Will move this to parameters settings soon
    df = df_list[1]['data']

    # arbitrary risk score
    df['score'] = ((df['dirvalue'] + df['vel_avg'] + df['vel_avg_change']) / df['timesum']) + \
            (df['rel_ang_diff_change'] / df['timesum']) + \
            ((df['rel_ang_diff'] - abs(df['rel_ang_diff_change'])) / df['timesum'])
    print("RISKY")
    print(df.head())
    exit()
    return df
#  CALC RISK END ------------------------------------------------------------------------
