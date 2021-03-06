"""Pipeline construction."""

from typing import Dict
from kedro.pipeline import Pipeline


from tough_on_turf.pipelines import prepare_data as p_d
from tough_on_turf.pipelines import group_metrics as g_m
from tough_on_turf.pipelines import visualize as v_p


# Here you can define your data-driven pipeline by importing your functions
# and adding them to the pipeline as follows:
#
# from nodes.data_wrangling import clean_data, compute_features
#
# pipeline = Pipeline([
#     node(clean_data, 'customers', 'prepared_customers'),
#     node(compute_features, 'prepared_customers', ['X_train', 'Y_train'])
# ])
#
# Once you have your pipeline defined, you can run it from the root of your
# project by calling:
#
# $ kedro run


def create_pipelines(**kwargs) -> Dict[str, Pipeline]:
    """Create the project's pipeline.

    Args:
        kwargs: Ignore any additional arguments added in the future.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.

    """
    prepare_data_pipeline = p_d.create_pipeline()  # outputs list of "data/02_intermediate/bodypart_list" files
    group_metrics_pipeline = g_m.create_pipeline()
    viz_pipeline = v_p.create_pipeline()

    return {
        "prepare_data": prepare_data_pipeline,
        "calc_group_metrics": group_metrics_pipeline,
        "viz_pipeline": viz_pipeline,
        "__default__": prepare_data_pipeline + group_metrics_pipeline + viz_pipeline,
        # "__default__": prepare_data_pipeline + group_metrics_pipeline,

    }

