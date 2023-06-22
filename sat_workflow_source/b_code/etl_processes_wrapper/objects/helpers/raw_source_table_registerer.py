import pandas

from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.origin_table_types import OriginTableTypes
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.raw_table_registerer import register_raw_table


def register_source_table(
        etl_processes_wrapper_registry,
        table: pandas.DataFrame,
        table_name: str,
        origin_table_type: OriginTableTypes,
        identifier_column_names: list) \
        -> None:
    register_raw_table(
        etl_processes_wrapper_registry=etl_processes_wrapper_registry,
        table=table,
        table_name=table_name,
        origin_table_type=origin_table_type,
        identifier_column_names=identifier_column_names)
# TODO Check columns are in the table
