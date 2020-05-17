from kedro.pipeline import Pipeline, node
from .nodes import surface, surface_length, surface_bodypart_length_w_null, surface_bodypart_length, graph_injury


def create_pipeline(**kwargs):
    return Pipeline(
        [
            # node(
            #     func=surface,
            #     inputs=["injury_record"],
            #     outputs=None,
            #     name="surface_graph",
            # ),
            node(
                func=surface_length,
                inputs=["injury_record"],
                outputs=None,
                name="surface_length_graph",
            ),
            node(
                func=surface_bodypart_length_w_null,
                inputs=["injury_record"],
                outputs=None,
                name="surface_bodypart_length_w_null_graph",
            ),
            node(
                func=surface_bodypart_length,
                inputs=["injury_metrics", "injury_record"],
                outputs=None,
                name="surface_bodypart_length_graph",
            ),
        ]
    )
