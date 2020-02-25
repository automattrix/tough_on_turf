import os
import pandas as pd
import contextlib
import numpy as np
from sklearn import preprocessing


def _extract_playerkey(input_playkey):
    try:
        tmp_key = str(input_playkey).split('-')
        player_key = tmp_key[0]
    except IndexError as error:
        print(error)
        player_key = None
    return player_key


def _write_h5(params):
    for chunk in pd.read_csv(params["data_path_in"], chunksize=params["chunksize"]):
        chunk['PlayerKey'] = chunk['PlayKey'].apply(_extract_playerkey)
        unique_players = chunk['PlayerKey'].unique()
        print(unique_players)

        for player in unique_players:
            keyname = f'player_{player}'
            df = chunk.loc[chunk['PlayerKey'] == player]
            df.to_hdf('./data/02_intermediate/nfl_trackdata.h5', append=True,
                      key=keyname, min_itemsize=100, complevel=params["compression_level"])


# TODO add additional operations for HDF generation (overwrite, new version...)
def generate_h5(params):
    if params["run_h5"]:
        print("Regenerating HDF output file...")
        exit()
        with contextlib.suppress:
            os.remove(params["data_path_out"])

        _write_h5(params=params)

    else:
        print("Not generating new HDF output file")


def _list_files(in_path, directory):
    tmp_csv_files = os.listdir(directory)
    csv_files = [f'{in_path}{csv_file}' for csv_file in tmp_csv_files]
    return csv_files


def _write_csv(csv_list, csv_out):
    tmp_out = open(csv_out, 'w')
    for i in csv_list:
        tmp_out.write(f'{i},\n')
    tmp_out.close()


def list_player_csvs(params):
    print("aggregating player data")
    lof = _list_files(in_path=params["data_path_in"], directory=f'{params["data_path_in"]}/{params["bodypart"]}')
    output_path = f'{params["data_path_out"]}/{params["bodypart"]}_list.csv'
    print(output_path)
    _write_csv(csv_list=lof, csv_out=output_path)
    return output_path


def _create_injury_dict(df, params):
    injury_dict = {}
    unique_bodyparts = df['BodyPart'].unique()
    df_unique_bodyparts = [_loc_dataframe(input_df=df, column='BodyPart', key=bodypart) for bodypart in unique_bodyparts]

    # Write injury bodypart DataFrames to csv
    for df_bodypart in df_unique_bodyparts:
        _df_to_csv(df=df_bodypart, output_dir=params["data_path_out"],
                   name=(str(df_bodypart['BodyPart'].iloc[0]).lower()) + '_injury.csv')

    return injury_dict


def _df_to_csv(df, output_dir, name):
    output_path = os.path.join(output_dir, name)
    print("Writing: {}".format(output_path))
    df.to_csv(output_path)


def _loc_dataframe(input_df, column, key):
    df = input_df.loc[input_df[column] == key]
    return df


# Writes './data/02_intermediate/bodypart/bodypart_injury.csv'
def prep_injury_data(df, params):
    player_injury_raw = df
    player_injury = player_injury_raw.dropna()
    _create_injury_dict(df=player_injury, params=params)

# ---------------


def _split_x_comp(xy_comp):
    x = xy_comp[0]
    return x


def _split_y_comp(xy_comp):
    y = xy_comp[1]
    return y


def _calculate_vector_components(x, y, o, mag):
    o_rad = np.deg2rad(o)
    x_comp = (mag * (np.cos(o_rad)))
    y_comp = (mag * (np.sin(o_rad)))
    #print("vector output")
    #print(x, x_comp, y, y_comp, o)

    return x_comp, y_comp


def _normalize_velocity(df):
    min_max_scaler = preprocessing.MinMaxScaler()
    try:
        x = df[['velocity_change']].values.astype(float)
        x_scaled = min_max_scaler.fit_transform(x)
        x_list = map(lambda x: x[0], x_scaled)
        x_series = pd.Series(x_list)
        # Add back the index
        x_series.index = df.index
        return x_series
    except:
        pass


def _velocity_pos_neg(velocity):
    if float(velocity) == 0:
        vel_change = "None"
    elif float(velocity) < 0:
        vel_change = "Negative"
    elif float(velocity) > 0:
        vel_change = "Positive"

    return vel_change


class NFLPlayer:
    def __init__(self, df, bodypart, params):
        self.params = params
        self.df = df
        self.bodypart = bodypart
        self.playerkey = 'player_' + str(df['PlayerKey'].iloc[0])
        self.injuryplay = df['PlayKey'].iloc[0]
        self.output_csv = f"{self.params['data_path_out']}/{self.bodypart}/{self.playerkey}_custom.csv"

        # self.injury_playdata = self.injury_play_dfs()  # THIS IS JUST THE INJURY PLAY DATA

    def check_output(self):
        if os.path.exists(self.output_csv):
            print(f"CSV exists: {self.output_csv}")
            return True
        else:
            return False

    def generate_playdata(self):
        # Add the distance and velocity columns
        playdata = self.df_custom_columns()  # THIS IS ALL THE PLAYDATA

        if not os.path.exists(self.output_csv):
            playdata.to_csv(self.output_csv)
        else:
            pass

    def load_playdata(self):
        # Load all playdata for this player
        play_df = pd.read_hdf(self.params["data_path_in"], key=self.playerkey)
        return play_df

    def df_custom_columns(self):
        playdata_raw = self.load_playdata()
        df = playdata_raw.copy()
        playdata_raw = None

        # Distance
        df['x_change'] = df['x'].diff().fillna(0)
        df['y_change'] = df['y'].diff().fillna(0)
        df['distance'] = np.sqrt(np.power(df['x_change'], 2) + np.power(df['y_change'], 2))

        # Time
        df['time_interval'] = df['time'].diff().fillna(0)

        # Velocity
        df['velocity'] = (df['distance'] / df['time_interval']).astype(float).fillna(0)
        df['velocity_change'] = df['velocity'].diff().fillna(0)
        df['pos_neg_vel'] = df['velocity_change'].apply(_velocity_pos_neg)

        # Calculate the x y components of a vector to graph the arrows for player orientation
        df['xy_comp'] = df.apply(lambda i: _calculate_vector_components(x=i['x'], y=i['y'], o=i['o'], mag=3), axis=1)
        df['x_comp'] = df['xy_comp'].apply(_split_x_comp)
        df['y_comp'] = df['xy_comp'].apply(_split_y_comp)

        # Calculate the x y components of a vector to graph the arrows for player direction
        df['xy_comp_dir'] = df.apply(lambda i: _calculate_vector_components(x=i['x'], y=i['y'], o=i['dir'], mag=3), axis=1)
        df['x_comp_dir'] = df['xy_comp_dir'].apply(_split_x_comp)
        df['y_comp_dir'] = df['xy_comp_dir'].apply(_split_y_comp)

        return df

    def injury_play_dfs(self):  # Currently not using this, but will in future commits
        df = self.playdata.loc[self.playdata['PlayKey'] == self.injuryplay]
        df_keys = ['Negative', 'Positive', 'None']

        play_dfs = []

        for key in df_keys:
            tmp_df = df.loc[df['pos_neg_vel'] == key]
            tmp_df = tmp_df.copy()
            tmp_df['veltype'] = key
            play_dfs.append(tmp_df)
            tmp_df = None

        final_play_dfs = []
        for play_df in play_dfs:
            play_df = play_df.copy()
            try:
                velocity_norm = _normalize_velocity(df=play_df)
                play_df['norm_velocity_change'] = velocity_norm
                final_play_dfs.append(play_df)
                #print(play_df.head())
                play_df = None
            except:
                pass

        return df, final_play_dfs


def _load_injury_df(bodypart, params):
    csv_path = (params["data_path_search"] + bodypart + '_injury.csv')
    df = pd.read_csv(csv_path)
    return df


def _prep_unique_players(df):
    unique_players = df['PlayerKey'].unique()

    return unique_players


# Get injury keys
def _discover_injury(data_dir):
    injury_csvs_raw = [x for x in os.listdir(data_dir) if not str(x).startswith('.git')]
    print(injury_csvs_raw)
    injury_keys = [str(tmp_injury).split('_')[0].strip() for tmp_injury in injury_csvs_raw]
    return injury_keys


def get_x_y_start_end(df):
    start_pt = df[['x', 'y']].iloc[0]
    end_pt = df[['x', 'y']].iloc[-1]

    return start_pt, end_pt


def generate_custom_csv(params):
    # Discover injury keys from csv files in directory
    injury_keys = _discover_injury(data_dir=params["data_path_search"])
    print("Detected injuries: {}".format(injury_keys))

    for injury_key in injury_keys:
        df = _load_injury_df(bodypart=injury_key, params=params)
        print(df.keys())

        unique_players = _prep_unique_players(df=df)
        for unique_player in unique_players:
            player_df = df.loc[df['PlayerKey'] == unique_player]
            player = NFLPlayer(df=player_df, bodypart=injury_key, params=params)
            print(player.playerkey)
            if player.check_output():  # generate if doesn't exist
                player = None
            else:
                player.generate_playdata()
                player = None
