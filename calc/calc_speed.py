import pandas as pd


def calc_avg(df):
    unique_velocities = df['pos_neg_vel'].unique()
    #print(unique_velocities)
    avg_total_vel = df['velocity'].mean()

    tmp_dict = {}

    #print(avg_total_vel)
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

    #print(tmp_dict.keys())

    return tmp_dict

