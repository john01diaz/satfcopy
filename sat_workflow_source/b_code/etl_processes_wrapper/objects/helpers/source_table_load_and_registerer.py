import os.path
import pandas
from deltalake import DeltaTable
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message

from sat_workflow_source.b_code.etl_processes.etl_layers.python.runners.helpers.dataframe_columns_to_default_value_setter import \
    set_dataframe_columns_to_default_value
from sat_workflow_source.b_code.etl_processes.etl_layers.python.runners.helpers.dataframe_columns_to_lowercase_renamer import \
    rename_dataframe_columns_to_lowercase
from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.origin_table_types import OriginTableTypes
from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.usage_table_types import UsageTableTypes
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.enum_from_string.enum_getter import get_enum
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.python_schema_enum_from_pandas_dataframe_getter import \
    get_python_schema_enum_from_pandas_dataframe
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.raw_source_table_registerer import \
    register_source_table
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.sql_create_table_statement_from_pandas_dataframe_getter import \
    get_sql_create_table_statement_from_pandas_dataframe
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.table_indexer import index_table
from sat_workflow_source.b_code.etl_processes_wrapper.objects.table_configurations import TableConfigurations


def load_and_register_source_table(
        etl_processes_wrapper_registry,
        table_configuration: TableConfigurations) \
        -> None:
    match table_configuration.source_extension_type:
        case 'snappy.parquet':
            table = \
                __get_parquet_table(
                    etl_processes_wrapper_registry=etl_processes_wrapper_registry,
                    table_configuration=table_configuration)

        case 'xlsx':
            table = \
                __get_xlsx_table(
                    etl_processes_wrapper_registry=etl_processes_wrapper_registry,
                    table_configuration=table_configuration)

        case _:
            log_message(
                'WARNING - table not loaded (no source info) - ' + table_configuration.table_name)

            return

    sql_create_statement = \
        get_sql_create_table_statement_from_pandas_dataframe(
            dataframe=table,
            table_name=table_configuration.table_name)

    key = \
        (table_configuration.table_name, OriginTableTypes.SOURCE)

    etl_processes_wrapper_registry.raw_and_bie_sub_register.sql_create_table_statements[key] = \
        sql_create_statement

    # TODO: For the time being can we comment this out when not needed.
    rename_dataframe_columns_to_lowercase(
        dataframe=table)

    python_schema_enum_string = \
        get_python_schema_enum_from_pandas_dataframe(
            dataframe=table,
            table_name=table_configuration.table_name)

    etl_processes_wrapper_registry.raw_and_bie_sub_register.python_schema_enums[key] = \
        python_schema_enum_string
    
    if table_configuration.defaulted_process_table_columns:
        set_dataframe_columns_to_default_value(
            dataframe=table,
            column_names=table_configuration.defaulted_process_table_columns)

    identifier_column_names = \
        table_configuration.bie_identifying_columns

    if table_configuration.origin_table_type == OriginTableTypes.GENERATED:
        origin_table_type = OriginTableTypes.SOURCE

    else:
        origin_table_type = table_configuration.origin_table_type

    register_source_table(
        etl_processes_wrapper_registry=etl_processes_wrapper_registry,
        table=table,
        table_name=table_configuration.table_name,
        origin_table_type=origin_table_type,
        identifier_column_names=identifier_column_names)

    if table_configuration.origin_table_type == OriginTableTypes.SOURCE_AFTER:
        index_table(
            etl_processes_wrapper_registry=etl_processes_wrapper_registry,
            table_name=table_configuration.table_name,
            origin_table_type=table_configuration.origin_table_type,
            usage_table_type=UsageTableTypes.COMPARE_INPUT,
            process_name='bieize')

    if table_configuration.origin_table_type == OriginTableTypes.SOURCE_BEFORE:
        index_table(
            etl_processes_wrapper_registry=etl_processes_wrapper_registry,
            table_name=table_configuration.table_name,
            origin_table_type=table_configuration.origin_table_type,
            usage_table_type=UsageTableTypes.COMPARE_INPUT,
            process_name='bieize')

    log_message(
        'INFO - table loaded - ' + table_configuration.table_name)


def __get_parquet_table(
        etl_processes_wrapper_registry,
        table_configuration: TableConfigurations) \
        -> pandas.DataFrame:
    etl_root_folder_path \
         = etl_processes_wrapper_registry.owning_etl_processes_wrapper_universe.etl_processes_wrapper_configuration.etl_root_folder_path

    name_components = \
        table_configuration.table_name.split(
            '.')

    table_name = \
        str(
            name_components[-1])

    absolute_table_name_folder_path = \
        os.path.join(
            etl_root_folder_path,
            'collect',
            table_configuration.table_source_relative_path,
            table_configuration.table_source_folder,
            table_name)

    if table_configuration.process_table_columns_filter:
        enum = \
            get_enum(
                enum_name=table_configuration.process_table_columns_filter)
        
        columns = \
            [column.value for column in enum]

        table = \
            pandas.read_parquet(
                absolute_table_name_folder_path,
                engine='pyarrow',
                columns=columns)

        return \
            table

    delta_table = \
        DeltaTable(
            absolute_table_name_folder_path)

    table = \
        delta_table.to_pyarrow_table().to_pandas()

    return \
        table


def __get_xlsx_table(
        etl_processes_wrapper_registry,
        table_configuration: TableConfigurations) \
        -> pandas.DataFrame:
    etl_root_folder_path \
        = etl_processes_wrapper_registry.owning_etl_processes_wrapper_universe.etl_processes_wrapper_configuration.etl_root_folder_path

    absolute_file_path = \
        os.path.join(
            etl_root_folder_path,
            'collect',
            table_configuration.table_source_relative_path,
            table_configuration.table_source_folder,
            table_configuration.table_name + '.xlsx')

    table = \
        pandas.read_excel(
            absolute_file_path,
            sheet_name=table_configuration.loader_excel_sheet_name,
            dtype=str,
            na_filter=False)

    return \
        table
