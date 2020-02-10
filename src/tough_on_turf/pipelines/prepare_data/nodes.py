import os


def _list_files(in_path, directory):
    tmp_csv_files = os.listdir(directory)
    csv_files = [f'{in_path}{csv_file}' for csv_file in tmp_csv_files]
    return csv_files


def _write_csv(csv_list, csv_out):
    tmp_out = open(csv_out, 'w')
    for i in csv_list:
        tmp_out.write(f'{i},\n')
    tmp_out.close()


def preprocess_player_files(p_params):
    print("aggregating player data")
    lof = _list_files(in_path=p_params["data_path_in"], directory=f'{p_params["data_path_in"]}/{p_params["bodypart"]}')
    output_path = f'{p_params["data_path_out"]}/{p_params["bodypart"]}_list.csv'
    print(output_path)
    _write_csv(csv_list=lof, csv_out=output_path)
    return output_path
