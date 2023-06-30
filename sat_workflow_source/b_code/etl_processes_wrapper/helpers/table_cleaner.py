from typing import Optional
from pandas import DataFrame
from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.satf_constants import DEFAULT_NULL_VALUE


def clean_source_table(
        source_table: DataFrame) \
        -> Optional[DataFrame]:
    cleaned_source_table = \
        source_table.replace(
            str(),
            DEFAULT_NULL_VALUE)

    return \
        cleaned_source_table
