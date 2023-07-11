from numpy import nan
from pandas import DataFrame
from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.satf_constants import B_DEFAULT_ISNULL


def migrate_table_to_bclearer_data_policy(
        table: DataFrame) \
        -> DataFrame:
    cleaned_table = \
        table.copy(
            deep=True)

    # cleaned_table = \
    #     table.replace(
    #         nan,
    #         B_DEFAULT_ISNULL,
    #         regex=True)

    return \
        cleaned_table
