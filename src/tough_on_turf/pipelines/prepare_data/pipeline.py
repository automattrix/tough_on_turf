from kedro.pipeline import Pipeline, node
from .nodes import preprocess_player_files, generate_h5


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=generate_h5,
                inputs=["params:data_prep"],
                outputs="csv_path",
                name="list_player_csvs",
            ),
            node(
                func=preprocess_player_files,
                inputs=["params:players_injury"],
                outputs="csv_path",
                name="list_player_csvs",
            ),
        ]
    )

