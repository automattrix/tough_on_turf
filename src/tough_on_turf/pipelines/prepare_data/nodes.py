import os
import pandas as pd
import h5py

def _list_files(in_path, directory):
    tmp_csv_files = os.listdir(directory)
    csv_files = [f'{in_path}{csv_file}' for csv_file in tmp_csv_files]
    return csv_files

def _write_csv(csv_list, csv_out):
    tmp_out = open(csv_out, 'w')
    for i in csv_list:
        tmp_out.write(f'{i},\n')
    tmp_out.close()


def preprocess_player_files(params):
    print("aggregating player data")
    lof = _list_files(in_path=params["data_path_in"], directory=f'{params["data_path_in"]}/{params["bodypart"]}')
    output_path = f'{params["data_path_out"]}/{params["bodypart"]}_list.csv'
    print(output_path)
    _write_csv(csv_list=lof, csv_out=output_path)
    return output_path


def _extract_playerkey(input_playkey):
    try:
        tmp_key = str(input_playkey).split('-')
        player_key = tmp_key[0]
    except:
        player_key = None
    return player_key

def generate_h5(params):

    if params["run_h5"]:
        # TODO change this file check behaviour. Method to be set in params (overwrite, move, delete, etc)
        if os.path.exists(params["data_path_out"]):
            exit()
        else:
            for chunk in pd.read_csv(params["data_path_in"], chunksize=params["chunksize"]):
                chunk['PlayerKey'] = chunk['PlayKey'].apply(_extract_playerkey)
                # print(chunk)
                unique_players = chunk['PlayerKey'].unique()
                print(unique_players)
                for player in unique_players:
                    keyname = f'player_{player}'
                    # print(keyname)
                    df = chunk.loc[chunk['PlayerKey'] == player]
                    # print(df)
                    df.to_hdf('./data/02_intermediate/nfl_trackdata.h5', append=True,
                              key=keyname, min_itemsize=100, complevel=params["compression_level"])


