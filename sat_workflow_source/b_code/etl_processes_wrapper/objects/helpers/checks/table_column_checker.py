from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from pandas import DataFrame

from sat_workflow_source.b_code.etl_schemas.silver_stage.silver_table_name_to_schema_mappings import SILVER_TABLE_NAME_TO_SCHEMA_MAPPINGS


def check_table_columns(
        table: DataFrame,
        table_name: str) \
        -> None:
    
    if table_name not in SILVER_TABLE_NAME_TO_SCHEMA_MAPPINGS.keys():
        return

    # TODO: Add checks for all stages - if exists
    column_names = \
        SILVER_TABLE_NAME_TO_SCHEMA_MAPPINGS[table_name]
    
    enum_columns = \
        {column.value
         for column
         in column_names}
    
    df_columns = \
        set(
            table.columns)

    message =  \
        (
            'Column name checks: ' +
            table_name +
            ' ' +
            str(df_columns == enum_columns))

    log_message(
        message=message)
