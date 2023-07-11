from pandas import DataFrame
from sat_workflow_source.b_code.etl_processes_wrapper.helpers.data_policy_to_table_applier import migrate_table_to_bclearer_data_policy


# todo rename filename
def migrate_output_table_to_bclearer_data_policy(
        output_table: DataFrame) \
        -> DataFrame:
    cleaned_output_table = \
        migrate_table_to_bclearer_data_policy(
            table=output_table)

    return \
        cleaned_output_table
