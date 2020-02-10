from kedro.pipeline import Pipeline, node
from .nodes import preprocess_player_files


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=preprocess_player_files,
                inputs=["params:players_injury"],
                outputs="csv_path",
                name="list_player_csvs",
            ),
        ]
    )

