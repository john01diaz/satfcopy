import os.path

import pandas
from nf_common_source.code.services.b_dictionary_service.objects.table_b_dictionaries import TableBDictionaries
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.input_output_service.delimited_text.dataframe_dictionary_to_csv_files_writer \
    import write_dataframe_to_csv_file
from nf_common_source.code.services.input_output_service.delimited_text.table_as_dictionary_to_csv_exporter import \
    export_table_as_dictionary_to_csv
from nf_common_source.code.services.reporting_service.reporters.log_file import LogFiles

from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.origin_table_types import OriginTableTypes
from sat_workflow_source.b_code.etl_processes_wrapper.objects.bie_sub_registers import BieSubRegisters


def report_bie(
        etl_processes_wrapper_registry) \
        -> None:
    from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_registries import \
        EtlProcessesWrapperRegistries

    if not isinstance(
            etl_processes_wrapper_registry,
            EtlProcessesWrapperRegistries):
        raise \
            TypeError

    __report_bie_sub_register(
        bie_sub_register=etl_processes_wrapper_registry.raw_and_bie_sub_register.source_bie_sub_register,
        origin_type=OriginTableTypes.SOURCE)

    __report_bie_sub_register(
        bie_sub_register=etl_processes_wrapper_registry.raw_and_bie_sub_register.generated_bie_sub_register,
        origin_type=OriginTableTypes.GENERATED)

    __report_bie_sub_register(
        bie_sub_register=etl_processes_wrapper_registry.raw_and_bie_sub_register.source_before_bie_sub_register,
        origin_type=OriginTableTypes.SOURCE_BEFORE)

    __report_bie_sub_register(
        bie_sub_register=etl_processes_wrapper_registry.raw_and_bie_sub_register.source_after_bie_sub_register,
        origin_type=OriginTableTypes.SOURCE_AFTER)


def __report_bie_sub_register(
        bie_sub_register: BieSubRegisters,
        origin_type: OriginTableTypes) \
        -> None:
    if not bie_sub_register.bie_tables:
        return

    for table_name, table in bie_sub_register.bie_tables.items():
        __export_table(
            dataframe=table,
            table_name=table_name,
            origin_type=origin_type)

    __export_table_as_dictionary_to_csv(
        table_b_dictionary=bie_sub_register.bie_id_register_table_b_dictionary,
        origin_type=origin_type)


def __export_table(
        dataframe: pandas.DataFrame,
        table_name: str,
        origin_type: OriginTableTypes) \
        -> None:
    folder_path = \
        os.path.join(
            LogFiles.folder_path,
            'csvs',
            'bie',
            origin_type.name)

    os.makedirs(
        folder_path,
        exist_ok=True)

    write_dataframe_to_csv_file(
        folder_name=folder_path,
        dataframe_name=table_name,
        dataframe=dataframe)


def __export_table_as_dictionary_to_csv(
        table_b_dictionary: TableBDictionaries,
        origin_type: OriginTableTypes) \
        -> None:
    table_as_dictionary = \
        dict()

    for row_b_dictionary_id_b_identity, row_b_dictionary \
            in table_b_dictionary.dictionary.items():
        table_as_dictionary[row_b_dictionary_id_b_identity] = \
            row_b_dictionary.dictionary

    folder_path = \
        os.path.join(
            LogFiles.folder_path,
            'csvs',
            'bie',
            origin_type.name)

    export_table_as_dictionary_to_csv(
        table_as_dictionary=table_as_dictionary,
        output_folder=Folders(folder_path),
        output_file_base_name=table_b_dictionary.table_name)
