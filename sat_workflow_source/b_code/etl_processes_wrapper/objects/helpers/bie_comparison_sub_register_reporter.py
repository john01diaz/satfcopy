import os.path
import pandas
from nf_common_source.code.services.b_dictionary_service.objects.table_b_dictionaries import TableBDictionaries
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.input_output_service.delimited_text.dataframe_dictionary_to_csv_files_writer \
    import write_dataframe_to_csv_file
from nf_common_source.code.services.input_output_service.delimited_text.table_as_dictionary_to_csv_exporter import \
    export_table_as_dictionary_to_csv
from nf_common_source.code.services.reporting_service.reporters.log_file import LogFiles
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message

from sat_workflow_source.b_code.etl_processes_wrapper.objects.bie_comparisons_sub_registers import \
    BieComparisonsSubRegisters


def report_bie_comparison_sub_register(
        bie_comparisons_register: BieComparisonsSubRegisters) \
        -> None:
    table_name = \
        'etl_analysis_bie_ids_comparison_summary'

    if table_name not in bie_comparisons_register.comparison_tables:
        log_message(
            'WARNING - No etl_analysis_bie_ids_comparison_summary table in bie comparison sub register')

        return

    table = \
        bie_comparisons_register.comparison_tables[table_name]

    __export_table(
        dataframe=table,
        table_name=table_name)

    __export_table_b_dictionary_to_csv(
        table_b_dictionary=bie_comparisons_register.comparison_tables['etl_analysis_bie_ids_comparison'])


def __export_table(
        dataframe: pandas.DataFrame,
        table_name: str) \
        -> None:
    folder_path = \
        os.path.join(
            LogFiles.folder_path,
            'csvs',
            'bie_comparison')

    os.makedirs(
        folder_path,
        exist_ok=True)

    write_dataframe_to_csv_file(
        folder_name=folder_path,
        dataframe_name=table_name,
        dataframe=dataframe)


def __export_table_b_dictionary_to_csv(
        table_b_dictionary: TableBDictionaries) \
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
            'bie_comparison')

    export_table_as_dictionary_to_csv(
        table_as_dictionary=table_as_dictionary,
        output_folder=Folders(folder_path),
        output_file_base_name=table_b_dictionary.table_name)
