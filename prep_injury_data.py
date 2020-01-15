from sklearn import preprocessing

import pandas as pd
import numpy as np
from matplotlib import cm
import matplotlib.pyplot as plt
#import seaborn as sns
import bokeh
import os


DATA_PLAYLIST = pd.read_csv('../ds_venv/nfl-playing-surface-analytics/PlayList.csv')
PLAYER_INJURY_PATH = '../ds_venv/nfl-playing-surface-analytics/InjuryRecord.csv'


def df_to_csv(df, output_dir, name):
    output_path = os.path.join(output_dir, name)
    print("Writing: {}".format(output_path))
    df.to_csv(output_path)


def loc_dataframe(input_df, column, key):
    df = input_df.loc[input_df[column] == key]
    return df


def load_injury():
    data_injury = pd.read_csv(PLAYER_INJURY_PATH)
    null_injury = data_injury[data_injury.isna().any(axis=1)]
    data_injury.dropna(inplace=True)
    return data_injury, null_injury


def create_injury_dict(df):
    injury_dict = {}
    unique_bodyparts = df['BodyPart'].unique()
    df_unique_bodyparts = [loc_dataframe(input_df=df, column='BodyPart', key=bodypart) for bodypart in unique_bodyparts]

    # Write injury bodypart DataFrames to csv
    for df_bodypart in df_unique_bodyparts:
        df_to_csv(df=df_bodypart, output_dir='./data/', name=(str(df_bodypart['BodyPart'].iloc[0]).lower()) + '_injury.csv')

    return injury_dict


def main():
    # Load player injury data, separate out null data
    player_injury_raw = load_injury()
    player_injury = player_injury_raw[0]
    null_injury = player_injury_raw[1]
    injury_dict = create_injury_dict(df=player_injury)



main()