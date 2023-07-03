import os.path
import pandas
from deltalake import DeltaTable
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.enum_from_string.enum_getter import get_enum
from sat_workflow_source.b_code.etl_processes_wrapper.objects.table_configurations import TableConfigurations


def get_parquet_table(
        etl_processes_wrapper_registry,
        table_configuration: TableConfigurations) \
        -> pandas.DataFrame:
    etl_root_folder_path \
        = etl_processes_wrapper_registry.owning_etl_processes_wrapper_universe.etl_processes_wrapper_configuration.etl_root_folder_path

    if table_configuration.alternative_table_name:
        table_name = \
            table_configuration.alternative_table_name

    else:
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

    # table = \
    #     table[table['database_name'] == 'R_2016R3']

    return \
        table
