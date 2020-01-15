import os
import pandas as pd
from calc import calc_speed, calc_rotation

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

    #def calc_speed(self):
        #print("Calculating player speed")

    def calc_rotation_score_body(self):
        print("Calculating body rotation score")
        # For rotation score, if player rotation continues in the same direction, consider that a series
        # For each series, caluclate the difference between the start and end rotation
        # Give his value a score

    def calc_rotation_score_head(self):
        print("Calculating head orientation score")
        # For rotation score, if player rotation continues in the same direction, consider that a series
        # For each series, caluclate the difference between the start and end rotation
        # Give his value a score

    def compare_rotation(self):
        print("Comparing rotation")
        # Compare the body and head rotation series
        # WHen head and body are further apart, calculate a higher risk score

    def calc_fatigue(self):
        print("Calculating player fatigue")
        # As the play progresses, add a modifier score as a tiredness factor

    def calc_injury_length(self):
        print("Calculating length of injury")
        # After all other facotrs have been calculated, we'll group by injury length

    def calc_weather(self):
        print("Calculating weather for injuries")
        # Penultimately, we will compare the weather to the injury to see if there are any correlations

    def calc_playfield(self):
        print("Calculating playfield")
        # Lastly, we will compare the playfield


def list_files(directory):
    csv_files = os.listdir(directory)
    return csv_files


def main():
    print("aggregating player data")

    lof = list_files(directory='./player_csv/knee/')
    print(f"Found {len(lof)} files")

    for tmp_playerfile in lof:
        # Create path to file
        player_file = str('./player_csv/knee/' + tmp_playerfile.strip())

        # Initiate Player class
        player = Player(csv_path=player_file)
        print(player.player_data.head())

        # Get unique playkeys
        play_keys = player.playerkeys()

#
        # TODO create an empty table for writing results from the following calcuations
        # Calculate data for each play
        for play in play_keys:
            print(play)
            # Load the data for the play
            play_df = player.player_data.loc[player.player_data['PlayKey'] == play]

            # Calculate speed averages
            # speed = calc_speed.calc_avg(play_df)
            # print(speed)


            # Calculate body rotation

            body_rotation = calc_rotation.calc_rotation(play_df)

            exit()
            # Create a table of results for each play



if __name__ == '__main__':
    main()
