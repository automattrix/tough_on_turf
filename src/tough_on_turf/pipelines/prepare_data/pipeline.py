from kedro.pipeline import Pipeline, node
from .nodes import list_player_csvs, generate_h5, prep_injury_data, generate_custom_csv


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=generate_h5,
                inputs=["params:data_prep"],
                outputs=None,
                name="generate_h5",
            ),
            node(
                func=prep_injury_data,
                inputs=["injury_record", "params:prep_injury"],
                outputs=None,
                name="prep_injury_data",
            ),
            node(
                func=generate_custom_csv,  # # writes to './data/02_intermediate/player_injury_csvs/bodypart/
                inputs=["params:generate_custom_csv"],
                outputs=None,
                name="generate_custom_csv",
            ),
            node(
                func=list_player_csvs,
                inputs=["params:players_injury"],
                outputs="csv_paths_list",
                name="list_player_csvs",
            ),

        ]
    )

