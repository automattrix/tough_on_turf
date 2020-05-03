from kedro.pipeline import Pipeline, node
from .nodes import create_bodypart_df_list, calc_metrics, calc_events


def create_pipeline(**kwargs):
    return Pipeline(
        [
            node(
                func=create_bodypart_df_list,
                inputs="csv_paths_list",
                outputs="df_list",
                name="bodypart_df_list",
            ),
            # node(
            #     func=calc_metrics,
            #     inputs=["csv_paths_list", "params:calc_metrics"],
            #     outputs="o_dir_df_list",
            #     name="calc_metrics",
            # ),
            node(
                func=calc_events,
                inputs=["csv_paths_list", "injury_record", "params:calc_metrics"],
                outputs="o_dir_df_list",
                name="calc_metrics",
            ),
        ]
    )
