import pandas as pd
import numpy as np
import re
import matplotlib.pyplot as plt
import seaborn as sns
import os


def create_bodypart_df_list(csv_list):
    df_list = [pd.read_csv(csv_path) for csv_path in csv_list]
    return df_list


#  CALC SPEED
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


def calc_angle_diff(o, direction):
    o_diff = (360 - o)
    dir_diff = (360 - direction)
    abs_diff = abs(o_diff - dir_diff)
    tmp_angle_diff = 360 - abs_diff

    if tmp_angle_diff > 180:
        angle_diff = 360 - tmp_angle_diff
    else:
        angle_diff = tmp_angle_diff

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


def compare_joined_values(a, b):
    if a == b:
        return "same"
    else:
        return "opposite"


class Player:
    def __init__(self, csv_path, params):
        self.player_data_path = csv_path
        self.player_data = self.load_df()
        self.playerkey = self.player_data['PlayerKey'].iloc[0]
        self.current_group = 0
        self.params = params
        self.d_one_df = pd.DataFrame()
        self.d_two_df = pd.DataFrame()
        self.dir_df = pd.DataFrame()
        self.games_df = pd.DataFrame()

    def load_df(self):
        df = pd.read_csv(self.player_data_path)
        return df

    def playerkeys(self):
        print("Calculating playkeys")
        unique_playkeys = self.player_data['PlayKey'].unique()
        return unique_playkeys

    def series_test(self, d, groupvalue, dir, num_values):
        test = np.full((1, num_values), 1, dtype=int)
        s = pd.Series(test[0]).to_frame(name=f'{d}_dir')
        s[f'current_group_{d}'] = groupvalue
        s[f'{d}_dir'] = dir

        if d == 'd1':
            self.d_one_df = self.d_one_df.append(s)
        elif d == 'd2':
            self.d_two_df = self.d_two_df.append(s)
        else:
            print("nope")
            exit()

    def test_read_joined_values(self, df, d, group):
        # group_value d1_dir  group_value d2_dir
        # id
        # 1        331469   left       331496   left
        # 2        331469   left       331496   left
        # 3        331469   left       331496   left

        if d == 'd1':
            otherkey = 'd2'
        elif d == 'd2':
            otherkey = 'd1'
        else:
            exit("Invalid Key")

        tmp_df = df.loc[df[f'current_group_{d}'] == group]
        tmp_df = tmp_df.copy()
        tmp_df['compare'] = tmp_df[['d1_dir', 'd2_dir']].apply(
            lambda x: compare_joined_values(a=x[f'{d}_dir'], b=x[f'{otherkey}_dir']), axis=1)

        same_df = tmp_df.loc[tmp_df['compare'] == 'same']
        overlap_dir = len(same_df.index)

        return overlap_dir

    def calc_dir_change(self, params, groupdf, dfkey):
        # TODO rename function to reflect calculations inside -- not just direction changes
        df = groupdf

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
        relative_angle_diff = abs(
            relative_angle_max - relative_angle_min)  # Largest difference in angle between body, head

        # Calculate velocity
        vel_avg = df['velocity'].mean()
        vel_avg_change = df['velocity_change'].mean()
        vel_min = df['velocity'].min()
        vel_max = df['velocity'].max()
        vel_change_min = df['velocity_change'].min()
        vel_change_max = df['velocity_change'].max()

        tmp_df = pd.DataFrame()
        tmp_df['dirvalue'] = pd.Series(dir_change)
        tmp_df['dirtype'] = pd.Series(dfkey)
        tmp_df['timesum'] = pd.Series(time_sum)
        tmp_df['num_values'] = pd.Series(num_measurements)
        tmp_df['direction'] = pd.Series(group_direction)
        tmp_df['rel_ang_diff_start'] = pd.Series(relative_angle_diff_start)
        tmp_df['rel_ang_diff_end'] = pd.Series(relative_angle_diff_end)
        tmp_df['rel_ang_diff_change'] = pd.Series(relative_angle_diff_change)
        tmp_df['rel_ang_min'] = pd.Series(relative_angle_min)
        tmp_df['rel_ang_max'] = pd.Series(relative_angle_max)
        tmp_df['rel_ang_diff'] = pd.Series(relative_angle_diff)
        tmp_df['vel_avg'] = pd.Series(vel_avg)
        tmp_df['vel_avg_change'] = pd.Series(vel_avg_change)
        tmp_df['vel_min'] = pd.Series(vel_min)
        tmp_df['vel_max'] = pd.Series(vel_max)
        tmp_df['vel_change_min'] = pd.Series(vel_change_min)
        tmp_df['vel_change_max'] = pd.Series(vel_change_max)

        self.dir_df = self.dir_df.append(tmp_df)

    def calc_groups(self, current_direction, next_direction):

        # If player direction has not changed, use the same group value
        # Else, create new group value
        if current_direction == next_direction:
            return self.current_group
        else:
            self.current_group += 1
            return self.current_group

    def compare_rotation(self, df_list):
        print("Comparing body and head rotation")

        # d1 head
        d1 = df_list[0]
        # d2 body
        d2 = df_list[1]

        # Need the ID as a column, so reset the index
        d1['data'].reset_index(inplace=True)
        d1['data'].rename(columns={'index': 'id'}, inplace=True)

        d2['data'].reset_index(inplace=True)
        d2['data'].rename(columns={'index': 'id'}, inplace=True)

        d1['data'][['id', 'direction', 'num_values']].apply(lambda x: self.series_test(
            d='d1', groupvalue=x['id'], dir=x['direction'], num_values=x['num_values']), axis=1)

        d2['data'][['id', 'direction', 'num_values']].apply(lambda x: self.series_test(
            d='d2', groupvalue=x['id'], dir=x['direction'], num_values=x['num_values']), axis=1)

        self.d_one_df.reset_index(inplace=True)
        self.d_one_df.drop(['index'], inplace=True, axis=1)
        self.d_two_df.reset_index(inplace=True)
        self.d_two_df.drop(['index'], inplace=True, axis=1)

        tmp_combined_df = pd.concat([self.d_one_df, self.d_two_df], axis=1, sort=False)

        # COMPARE FROM DF
        # Temporarily only using d2 -- clean this up
        # d1_overlap = tmp_combined_df.apply(
        #    lambda x: self.test_read_joined_values(df=tmp_combined_df, d='d1', group=x['current_group_d1']), axis=1
        # )
        d2_overlap = tmp_combined_df.apply(
            lambda x: self.test_read_joined_values(df=tmp_combined_df, d='d2', group=x['current_group_d2']), axis=1
        )

        # d1['data']['overlap'] = d1_overlap
        d2['data']['overlap'] = d2_overlap

        # Temporarily commenting this out as I'm using d2 instead
        # d1['data']['overlap_pct'] = (d1['data']['overlap'] / d1['data']['num_values']) * 100
        d2['data']['overlap_pct'] = (d2['data']['overlap'] / d2['data']['num_values']) * 100
        self.d_one_df = pd.DataFrame()
        self.d_two_df = pd.DataFrame()

        return d1, d2

    def score(self, df_list):
        # Temporarily manually specifying the 'dir' df, instead of orientation df
        # Will move this to parameters settings soon
        df = df_list[1]['data']

        # arbitrary risk score, will probably revisit this
        df['score'] = ((df['dirvalue'] + df['vel_avg'] + df['vel_avg_change']) / df['timesum']) + \
                      (df['rel_ang_diff_change'] / df['timesum']) + \
                      ((df['rel_ang_diff'] - abs(df['rel_ang_diff_change'])) / df['timesum'])

        return df

    def metrics(self):
        score_output = open(f'./data/02_intermediate/risk_score_{self.playerkey}.csv', 'w')
        score_output.write('PlayKey, RiskScore, WeightedRiskScore\n')
        print("calculating class player metrics")
        for play in self.playerkeys():
            # Load the data for the play
            play_df = self.player_data.loc[self.player_data['PlayKey'] == play]

            o_dir_list = []
            print(play)
            for dfkey in self.params['df_keys']:
                df = play_df.copy()

                # Calculate relative difference in degrees between head and body orientation
                df['head_v_body_diff'] = df[['o', 'dir']].apply(lambda x: calc_angle_diff(
                    o=x['o'], direction=x['dir']), axis=1
                                                                )

                # Calculate difference in orientation between measurements
                df["delta"] = df[dfkey].diff().fillna(0)

                # Calculate left of right change in direction

                df['pos_neg_orientation'] = df['delta'].apply(pos_neg_orientation)

                # Create new column shifted up by 1 row to compare current dir measurement to next dir measurement

                df['direction_shift'] = df['pos_neg_orientation'].shift(periods=-1, fill_value="no change")

                # Calculate groups ----------
                # A direction value is considered to be in the same group if the player direction has not changed
                df['groups'] = df[['pos_neg_orientation', 'direction_shift']].apply(
                    lambda i: self.calc_groups(current_direction=i['pos_neg_orientation'],
                                               next_direction=i['direction_shift']), axis=1)

                # Calculate change in direction ---------
                unique_groups = df['groups'].unique()

                for group in unique_groups:
                    group_df = df.loc[df['groups'] == group]
                    # Calculate change in direction and angles, and write to database
                    self.calc_dir_change(params=self.params, groupdf=group_df, dfkey=dfkey)

                dir_df = self.dir_df.copy()
                dir_df.reset_index(inplace=True,)
                dir_df.drop(['index'], axis=1, inplace=True)

                # Find min and max dir change
                max_dir = dir_df['dirvalue'].max()
                max_duration = dir_df['timesum'].max()

                # Add additional columns, returns a dict, containing a DataFrame
                dir_dict = calc_pct_of_max(dir_changes=dir_df, maxdir=max_dir, maxduration=max_duration)
                o_dir_list.append(dir_dict)
            self.dir_df = pd.DataFrame()

            play_df = None
            print("headvbody")
            head_vs_body = self.compare_rotation(df_list=o_dir_list)
            print("risk")
            risk_score = self.score(df_list=head_vs_body)
            print("risk")
            weighted_score = (risk_score['score'] * risk_score['timesum']).sum() / risk_score['timesum'].sum()
            avg_score = risk_score['score'].mean()
            print(risk_score['score'].mean())
            print(weighted_score)

            score_output.write(f'{play},{avg_score},{weighted_score}\n')
        score_output.close()

    def score_play_events(self):
        # event_output = open(f'./data/02_intermediate/event_score_{self.playerkey}.csv', 'w')
        # event_output.write('PlayKey, RiskScore, WeightedRiskScore\n')
        print("calculating play events")

        combined_play_df_list = []  # List for all plays
        for play in self.playerkeys():
            # Load the data for the play
            play_df = self.player_data.loc[self.player_data['PlayKey'] == play]
            print(play)

            df = play_df.copy()
            # print(df.keys())

            # Add event to blank rows
            df['event'].ffill(inplace=True)
            df.dropna(inplace=True)  # important to drop AFTER ffill. Otherwise all rows missing an event are removed

            # List for all events in the current play
            tmp_event_df_list = []

            unique_events = df['event'].unique()
            for event in unique_events:
                df_event = df.loc[df['event'] == event]

                agg_df = _calc_event_aggregates(df=df_event)
                tmp_event_df_list.append(agg_df)

            # Combine all events
            combined_df = pd.concat(tmp_event_df_list)
            combined_df.reset_index(inplace=True)
            combined_df.drop(['index'], axis=1, inplace=True)
            # print(combined_df)
            combined_play_df_list.append(combined_df)
            # tmp_df_list = []

        # Combine all plays (could span multiple games)
        games_df = pd.concat(combined_play_df_list)
        games_df.reset_index(inplace=True)
        games_df.drop(['index'], axis=1, inplace=True)

        # Add column to determine which game
        games_df['game_key'] = games_df['playkey'].apply(_split_game)

        # Calculate percentage of max efforts
        games_df = _pct_effort_avg_vel(df=games_df)  # calc pct of max avg velocity

        self.games_df = games_df

        # Testing
        test = games_df.loc[games_df['game_key'] == "1"]
        # grpahevents = game_df.pivot("event", "playkey", "vel_avg")
        graphevents = test.pivot("event", "playkey", "pct_eff_vel_avg")
        # f, ax = plt.subplots(figsize=(30, 35))
        #sns.heatmap(graphevents, ax=ax, cmap='coolwarm', square=True, cbar=False)

        grid_kws = {"height_ratios": (.9, .005), "hspace": .01}
        f, (ax, cbar_ax) = plt.subplots(2, gridspec_kw=grid_kws, figsize=(20,10))
        ax = sns.heatmap(graphevents, ax=ax,
                         cbar_ax=cbar_ax,
                         cbar_kws={"orientation": "horizontal"},
                         square=True, cmap='coolwarm')
        # sns_heat = sns.heatmap(grpahevents, cmap='coolwarm')
        # figure = sns_heat.get_figure()
        f.savefig('./test_heat.png')

    # Determine if any of the playkeys overlap with an injury key
    def analyze_injury(self, injury_keys, injury_record):
        is_injured = list(set(self.playerkeys()).intersection(injury_keys))
        print(is_injured)

        injury_df = injury_record.copy()

        if len(is_injured) > 0:  # there should only be
            injury_play_df_list = []
            drop_keys = ['PlayerKey', 'GameID', 'PlayKey']

            for injury in is_injured:
                df_tmp = self.games_df.loc[self.games_df['playkey'] == injury]
                injury_row = injury_df.loc[injury_df['PlayKey'] == injury]
                injury_row = injury_row.copy()
                injury_row.drop(drop_keys, inplace=True, axis=1)
                injury_row_keys = injury_row.keys()

                # reset index before concat
                df_tmp.reset_index(inplace=True)
                injury_row.reset_index(inplace=True)

                # Concat injury row as new columns to df_tmp
                combined_df = pd.concat([df_tmp, injury_row], axis=1)
                combined_df = combined_df.copy()
                # Remove duplicate index
                combined_df.drop(['index'], inplace=True, axis=1)
                # Fill (copy) down injury values
                combined_df[injury_row_keys] = combined_df[injury_row_keys].ffill()

               # print(combined_df)

                injury_play_df_list.append(combined_df)

            if len(injury_play_df_list) > 1:
                all_injuries = pd.concat(injury_play_df_list)
            else:
                all_injuries = injury_play_df_list[0]

            print(all_injuries.head())
            csv_out = './data/03_primary/injury_metrics.csv'
            if os.path.exists(csv_out):
                all_injuries.to_csv(csv_out, mode='a', index=False, header=False)
            else:
                all_injuries.to_csv(csv_out, index=False)

            return all_injuries

        else:
            pass
        # if match, concat ro
        # w from injury record df


def injury_keys(injuryrecord):
    df_injury = injuryrecord.copy()
    injury_keys = df_injury['PlayKey'].unique()
    return injury_keys


def _join_injury_record(df):
    test = ''


def _pct_effort_avg_vel(df):
    avg_vel_max = df['vel_avg'].max()
    df['pct_eff_vel_avg'] = (df['vel_avg'] / avg_vel_max) * 100
    return df


def _split_game(playkey):
    game_key = playkey.split('-')[1]
    return game_key


def _calc_event_aggregates(df):

    agg_df = pd.DataFrame()
    agg_df['playkey'] = pd.Series(df['PlayKey'].iloc[0])
    agg_df['event'] = pd.Series(df['event'].iloc[0])
    # Velocity
    agg_df["vel_avg"] = pd.Series(df['velocity'].mean())
    agg_df["vel_min"] = pd.Series(df['velocity'].min())
    agg_df["vel_max"] = pd.Series(df['velocity'].max())

    # Velocity Change
    agg_df["vel_change_avg"] = pd.Series(df['velocity_change'].mean())
    agg_df["vel_change_min"] = pd.Series(df['velocity_change'].min())
    agg_df["vel_change_max"] = pd.Series(df['velocity_change'].max())

    # Distance
    agg_df["distance"] = pd.Series(df['distance'].sum())

    # Time
    agg_df["timesum"] = pd.Series(df['time_interval'].sum())

    return agg_df


def calc_metrics(df_csv_list, params):

    print(df_csv_list)
    # bodypart_list: df
    df_dict = _player_csv_dict(df_csv_list=df_csv_list)

    score_output = open('./data/02_intermediate/risk_score_tmp.csv', 'w')
    score_output.write('PlayKey, RiskScore, WeightedRiskScore\n')
    for bodypart_list, df in df_dict.items():

        print(bodypart_list)  # ankle_list
        for player_file in df.itertuples():
            player = Player(csv_path=player_file[1], params=params)
            #player.metrics()


def calc_events(df_csv_list, injuryrecord, params):
    print("Calculating play events")

    df_dict = _player_csv_dict(df_csv_list=df_csv_list)

    score_output = open('./data/02_intermediate/risk_score_tmp.csv', 'w')
    score_output.write('PlayKey, RiskScore, WeightedRiskScore\n')

    # Only some injuries were known to happen on a given play
    # Remove those that have no known playkey
    injury_record = injuryrecord.dropna()

    for bodypart_list, df in df_dict.items():
        # TODO Get all the players with injuries first, create a list of their playerkeys. Process separately
        print(bodypart_list)  # ankle_list
        for player_file in df.itertuples():
            player = Player(csv_path=player_file[1], params=params)
            player.score_play_events()
            player.analyze_injury(injury_keys=injury_keys(injury_record), injury_record=injury_record)




