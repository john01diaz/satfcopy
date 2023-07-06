from pandas import DataFrame
from sat_workflow_source.b_code.etl_processes_wrapper.helpers.table_cleaner import clean_table


def clean_output_table(
        output_table: DataFrame) \
        -> DataFrame:
    cleaned_output_table = \
        clean_table(
            table=output_table)

    return \
        cleaned_output_table
