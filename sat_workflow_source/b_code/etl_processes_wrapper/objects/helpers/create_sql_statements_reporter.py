import os.path
from nf_common_source.code.services.reporting_service.reporters.log_file import LogFiles
from sat_workflow_source.b_code.etl_processes_wrapper.objects.raw_and_bie_sub_registers import RawAndBieSubRegisters


def report_create_sql_statements(
        etl_processes_wrapper_registry) \
        -> None:
    from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_registries import \
        EtlProcessesWrapperRegistries

    if not isinstance(
            etl_processes_wrapper_registry,
            EtlProcessesWrapperRegistries):
        raise \
            TypeError

    __report_sub_register(
        sub_register=etl_processes_wrapper_registry.raw_and_bie_sub_register)


def __report_sub_register(
        sub_register: RawAndBieSubRegisters) \
        -> None:
    for table_name_and_origin_type, sql_create_table_statement in sub_register.sql_create_table_statements.items():
        __export_create_sql_statement(
            sql_create_table_statement=sql_create_table_statement,
            table_name=table_name_and_origin_type[0])


def __export_create_sql_statement(
        sql_create_table_statement: str,
        table_name: str) \
        -> None:
    folder_path = \
        os.path.join(
            LogFiles.folder_path,
            'sql_create_table_statements')

    os.makedirs(
        folder_path,
        exist_ok=True)

    output_sql_file_name = \
        table_name + '_' + '_sql_create_table_statement.sql'

    output_sql_file_path = \
        os.path.join(
            folder_path,
            output_sql_file_name)

    with open(output_sql_file_path, 'w') as f:
        f.write(sql_create_table_statement)
