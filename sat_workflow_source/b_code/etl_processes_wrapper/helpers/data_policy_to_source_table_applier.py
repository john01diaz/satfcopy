from pandas import DataFrame
from sat_workflow_source.b_code.etl_processes_wrapper.helpers.data_policy_to_table_applier import migrate_table_to_bclearer_data_policy


def migrate_source_table_to_bclearer_data_policy(
        source_table: DataFrame) \
        -> DataFrame:
    cleaned_source_table = \
        migrate_table_to_bclearer_data_policy(
            table=source_table)

    return \
        cleaned_source_table
