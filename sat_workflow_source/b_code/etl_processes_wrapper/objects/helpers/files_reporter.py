import os.path

from nf_common_source.code.services.input_output_service.delimited_text.dataframe_dictionary_to_csv_files_writer \
    import write_dataframe_to_csv_file
from nf_common_source.code.services.reporting_service.reporters.log_file import LogFiles
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message

from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.origin_table_types import OriginTableTypes
from sat_workflow_source.b_code.etl_processes_wrapper.objects.raw_and_bie_sub_registers import RawAndBieSubRegisters
from sat_workflow_source.b_code.etl_processes_wrapper.objects.b_tables import BTables


def report_files(
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
    for table_name_and_origin_type, table in sub_register.raw_sub_register.items():
        __export_table(
            table=table,
            table_name=table_name_and_origin_type[0],
            origin_type=table_name_and_origin_type[1])


def __export_table(
        table: BTables,
        table_name: str,
        origin_type: OriginTableTypes) \
        -> None:
    folder_path = \
        os.path.join(
            LogFiles.folder_path,
            'csvs',
            'raw',
            origin_type.name)

    os.makedirs(
        folder_path,
        exist_ok=True)

    dataframe = \
        table.table

    if dataframe is None:
        message = \
            'ERROR - table not found when reporting: ' + table_name + ' ' + origin_type.name

        log_message(
            message=message)

        return

    dataframe = \
        dataframe.fillna('null')

    write_dataframe_to_csv_file(
        folder_name=folder_path,
        dataframe_name=table_name,
        dataframe=dataframe)
