import os
import pandas as pd
import numpy as np
from sklearn import preprocessing
import graphing



DATA_INJURY_BODYPART_DIR = './data/'
PLAYER_DATA_PATH = '../ds_venv/nfl_trackdata.h5'


def split_x_comp(xy_comp):
    x = xy_comp[0]
    return x


def split_y_comp(xy_comp):
    y = xy_comp[1]
    return y


def calculate_vector_components(x, y, o, mag):
    o_rad = np.deg2rad(o)
    x_comp = (mag * (np.cos(o_rad)))
    y_comp = (mag * (np.sin(o_rad)))
    #print("vector output")
    #print(x, x_comp, y, y_comp, o)

    return x_comp, y_comp


def normalize_velocity(df):
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


def velocity_pos_neg(velocity):
    if float(velocity) == 0:
        vel_change = "None"
    elif float(velocity) < 0:
        vel_change = "Negative"
    elif float(velocity) > 0:
        vel_change = "Positive"

    return vel_change


class NFLPlayer:
    def __init__(self, df):
        self.df = df
        self.playerkey = 'player_' + str(df['PlayerKey'].iloc[0])
        self.injuryplay = df['PlayKey'].iloc[0]


        # Load all playdata for this player
        self.playdata_raw = self.load_playdata()

        # Add the distance and velocity columns
        self.playdata = self.df_custom_columns()  # THIS IS ALL THE PLAYDATA
        #self.injury_playdata = self.injury_play_dfs()  # THIS IS JUST THE INJURY PLAY DATA

    def load_playdata(self):
        play_df = pd.read_hdf('../ds_venv/nfl_trackdata.h5', key=self.playerkey)
        return play_df

    def df_custom_columns(self):
        df = self.playdata_raw.copy()

        # Distance
        df['x_change'] = df['x'].diff().fillna(0)
        df['y_change'] = df['y'].diff().fillna(0)
        df['distance'] = np.sqrt(np.power(df['x_change'], 2) + np.power(df['y_change'], 2))

        # Time
        df['time_interval'] = df['time'].diff().fillna(0)

        # Velocity
        df['velocity'] = (df['distance'] / df['time_interval']).astype(float).fillna(0)
        df['velocity_change'] = df['velocity'].diff().fillna(0)
        df['pos_neg_vel'] = df['velocity_change'].apply(velocity_pos_neg)

        # Calculate the x y components of a vector to graph the arrows for player orientation
        df['xy_comp'] = df.apply(lambda i: calculate_vector_components(x=i['x'], y=i['y'], o=i['o'], mag=3), axis=1)
        df['x_comp'] = df['xy_comp'].apply(split_x_comp)
        df['y_comp'] = df['xy_comp'].apply(split_y_comp)

        # Calculate the x y components of a vector to graph the arrows for player direction
        df['xy_comp_dir'] = df.apply(lambda i: calculate_vector_components(x=i['x'], y=i['y'], o=i['dir'], mag=3), axis=1)
        df['x_comp_dir'] = df['xy_comp_dir'].apply(split_x_comp)
        df['y_comp_dir'] = df['xy_comp_dir'].apply(split_y_comp)
        tmp_path = f"./player_csv/{self.playerkey}_custom.csv"
        if not os.path.exists(tmp_path):
            df.to_csv(f'./player_csv/{self.playerkey}_custom.csv')
        else:
            pass
        return df

    def current_play_df(self):
        df = self.playdata

    def injury_play_dfs(self):
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
                velocity_norm = normalize_velocity(df=play_df)
                play_df['norm_velocity_change'] = velocity_norm
                final_play_dfs.append(play_df)
                #print(play_df.head())
                play_df = None
            except:
                pass

        return df, final_play_dfs


def load_injury_df(bodypart):
    csv_path = (DATA_INJURY_BODYPART_DIR + bodypart + '_injury.csv')
    df = pd.read_csv(csv_path)
    return df


def prep_unique_players(df):
    unique_players = df['PlayerKey'].unique()

    return unique_players


# Get injury keys
def discover_injury(data_dir):
    injury_csvs_raw = os.listdir(data_dir)
    injury_keys = [str(tmp_injury).split('_')[0].strip() for tmp_injury in injury_csvs_raw]
    return injury_keys


def get_x_y_start_end(df):
    start_pt = df[['x', 'y']].iloc[0]
    end_pt = df[['x', 'y']].iloc[-1]

    return start_pt, end_pt


def main():
    # Discover injury keys from csv files in directory
    injury_keys = discover_injury(data_dir=DATA_INJURY_BODYPART_DIR)
    print("Detected injuries: {}".format(injury_keys))

    # Load test dictionary
    # TODO handle all injury dictionaries
    df = load_injury_df(bodypart=injury_keys[2])
    print(df.keys())

    unique_players = prep_unique_players(df=df)
    for unique_player in unique_players:
        player_df = df.loc[df['PlayerKey'] == unique_player]
        player = NFLPlayer(df=player_df)

        print(player.playerkey)
        player = None

        # GRAPH
        # graphing.graph_route_arrows(play_df=player.injury_playdata[0], name='{}_vector.jpg'.format(player.playerkey))
        # graphing.graph_route(play_df=player.injury_playdata[0], name='{}_full.jpg'.format(player.playerkey))
        # graphing.graph_norm_velocity(play_df=player.injury_playdata[1], name='{}_norm_velocity.jpg'.format(player.playerkey))



main()
