from pandas import DataFrame
from sat_workflow_source.b_code.etl_processes_wrapper.helpers.table_cleaner import clean_table


def clean_source_table(
        source_table: DataFrame) \
        -> DataFrame:
    cleaned_source_table = \
        clean_table(
            table=source_table)

    return \
        cleaned_source_table
