import pandas

from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.origin_table_types import OriginTableTypes
from sat_workflow_source.b_code.etl_processes_wrapper.objects.b_tables import BTables


def register_raw_table(
        etl_processes_wrapper_registry,
        table: pandas.DataFrame,
        table_name: str,
        origin_table_type: OriginTableTypes,
        identifier_column_names: list) \
        -> None:
    sub_register = \
        etl_processes_wrapper_registry.raw_and_bie_sub_register

    etl_table = \
        BTables(
            table=table,
            identifier_column_names=identifier_column_names)

    sub_register.raw_sub_register[(table_name, origin_table_type)] = \
        etl_table

# TODO Check columns are in the table
