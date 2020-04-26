import pandas as pd
import numpy as np


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

