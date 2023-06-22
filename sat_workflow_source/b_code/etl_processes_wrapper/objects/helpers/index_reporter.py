import os.path

from nf_common_source.code.services.input_output_service.delimited_text.dataframe_dictionary_to_csv_files_writer \
    import write_dataframe_to_csv_file
from nf_common_source.code.services.reporting_service.reporters.log_file import LogFiles
from pandas import DataFrame


def report_index(
        etl_processes_wrapper_registry) \
        -> None:
    from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_registries import \
        EtlProcessesWrapperRegistries

    if not isinstance(
            etl_processes_wrapper_registry,
            EtlProcessesWrapperRegistries):
        raise \
            TypeError

    __report_table(
        process_table_usages=etl_processes_wrapper_registry.raw_and_bie_sub_register.process_table_usages)


def __report_table(
        process_table_usages: list) \
        -> None:
    dataframe = \
        DataFrame(
            columns=['process_names', 'usage_table_types', 'origin_table_types', 'table_names'])
    
    for process_table_usage in process_table_usages:
        dataframe.loc[len(dataframe.index)] = \
            process_table_usage

    __export_table(
        dataframe=dataframe)


def __export_table(
        dataframe: DataFrame) \
        -> None:
    folder_path = \
        os.path.join(
            LogFiles.folder_path,
            'csvs')

    os.makedirs(
        folder_path,
        exist_ok=True)

    dataframe = \
        dataframe.fillna(
            'null')

    write_dataframe_to_csv_file(
        folder_name=folder_path,
        dataframe_name='process_table_usages',
        dataframe=dataframe)
