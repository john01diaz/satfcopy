import os.path
from sat_workflow_source.b_code.etl_processes_wrapper.objects.table_configurations import TableConfigurations


def get_absolute_path_for_table(
        etl_processes_wrapper_registry,
        table_configuration: TableConfigurations) \
        -> str:
    etl_source_root_folder = \
        etl_processes_wrapper_registry.owning_etl_processes_wrapper_universe.etl_processes_wrapper_configuration.etl_sources_root_folder

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

    absolute_path_for_table = \
        os.path.join(
            etl_source_root_folder.absolute_path_string,
            table_configuration.table_source_relative_path,
            table_configuration.table_source_folder,
            table_name)

    match table_configuration.source_extension_type:
        case 'xlsx':
            absolute_path_for_table = \
                absolute_path_for_table + '.xlsx'

        case 'csv':
            absolute_path_for_table = \
                absolute_path_for_table + '.csv'

        case 'aql':
            absolute_path_for_table = \
                absolute_path_for_table + '.csv'

    return \
        absolute_path_for_table
