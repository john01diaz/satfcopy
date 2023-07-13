import pandas
from sat_common_source.table_readers.helpers.absolute_path_for_table_getter import get_absolute_path_for_table
from sat_workflow_source.b_code.etl_processes_wrapper.objects.table_configurations import TableConfigurations


def get_aql_table(
        etl_processes_wrapper_registry,
        table_configuration: TableConfigurations) \
        -> pandas.DataFrame:
    absolute_path_for_table = \
        get_absolute_path_for_table(
            etl_processes_wrapper_registry=etl_processes_wrapper_registry,
            table_configuration=table_configuration)

    table = \
        pandas.read_csv(
            absolute_path_for_table,
            dtype=object,
            encoding='mbcs',
            keep_default_na=False,
            na_values=[''],
            sep=';')

    return \
        table
