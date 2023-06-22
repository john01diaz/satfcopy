from typing import List
from pandas import DataFrame
from sat_workflow_source.b_code.etl_processes.common.constants import DEFAULT_CELL_VALUE


def set_dataframe_columns_to_default_value(
        dataframe: DataFrame,
        column_names: List[str])\
        -> None:
    for column_name in column_names:
        dataframe[column_name] = \
            DEFAULT_CELL_VALUE
