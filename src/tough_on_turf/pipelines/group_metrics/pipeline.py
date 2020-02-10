from kedro.pipeline import Pipeline, node
from .nodes import calc_speed


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=calc_speed,
                inputs="companies",
                outputs="preprocessed_companies",
                name="preprocessing_companies",
            ),
        ]
    )