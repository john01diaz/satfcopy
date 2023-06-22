from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.origin_table_types import OriginTableTypes
from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.format_table_types import FormatTableTypes


def add_schema_to_schemas_report(
        schemas_report: dict,
        schema_dictionary: dict,
        table_name: str,
        origin_table_type: OriginTableTypes,
        format_table_type: FormatTableTypes):
    for index, column_report_dictionary in schema_dictionary.items():
        __add_column_report_to_schemas_report(
            schemas_report=schemas_report,
            column_report_dictionary=column_report_dictionary,
            table_name=table_name,
            origin_table_type=origin_table_type,
            format_table_type=format_table_type)


def __add_column_report_to_schemas_report(
        schemas_report: dict,
        column_report_dictionary: dict,
        table_name: str,
        origin_table_type: OriginTableTypes,
        format_table_type: FormatTableTypes):
    schemas_report_row_dictionary = \
        {
            'table_names': table_name,
            'column_names': column_report_dictionary['column_names'],
            'datatypes': column_report_dictionary['datatypes'],
            'origin_table_type': origin_table_type.value,
            'format_table_type': format_table_type.value
        }

    schemas_report[len(schemas_report)] = \
        schemas_report_row_dictionary
