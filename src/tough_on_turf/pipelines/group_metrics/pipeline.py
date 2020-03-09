from kedro.pipeline import Pipeline, node
from .nodes import create_bodypart_df_list, calc_metrics, compare_rotation, score


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=create_bodypart_df_list,
                inputs="csv_paths_list",
                outputs="df_list",
                name="bodypart_df_list",
            ),
            node(
                func=calc_metrics,
                inputs=["csv_paths_list", "params:calc_metrics"],
                outputs="o_dir_df_list",
                name="calc_metrics",
            ),
            node(
                func=compare_rotation,
                inputs=["o_dir_df_list", "params:compare_rotation"],
                outputs="rotation_df_list",
                name="compare_rotation",
            ),
            node(
                func=score,
                inputs="rotation_df_list",
                outputs="risk_df",
                name="risk_score",
            ),
        ]
    )
