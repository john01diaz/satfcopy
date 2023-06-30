import os
from nf_common_source.code.services.input_output_service.excel.excel_write import save_table_in_excel
from nf_common_source.code.services.reporting_service.reporters.log_file import LogFiles
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from pandas import DataFrame

from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.satf_constants import DEFAULT_NULL_VALUE


def export_table_to_excel(
        bie_table_id: str,
        input_tables: dict,
        table_configurations: list,
        code_process_name: str) \
        -> DataFrame:
    folder_path = \
        os.path.join(
            LogFiles.folder_path,
            'xlsx',
            code_process_name)

    os.makedirs(
        folder_path,
        exist_ok=True)

    table_name = \
        next(iter(input_tables))

    dataframe = \
        input_tables[table_name]

    if dataframe is None:
        message = \
            'ERROR - table not found when reporting: ' + table_name

        log_message(
            message=message)

        return

    dataframe = \
        dataframe.fillna('null')

    dataframe.replace(
        DEFAULT_NULL_VALUE,
        'null',
        inplace=True)

    full_filename = \
        os.path.join(
            folder_path,
            table_name + '.xlsx')

    sheet_name = \
        [
            table_configuration['loader_excel_sheet_names']
            for table_configuration in table_configurations
            if table_configuration['bie_table_ids'] == bie_table_id
        ][0]

    save_table_in_excel(
        table=dataframe,
        full_filename=full_filename,
        sheet_name=sheet_name)

    return \
        dataframe
