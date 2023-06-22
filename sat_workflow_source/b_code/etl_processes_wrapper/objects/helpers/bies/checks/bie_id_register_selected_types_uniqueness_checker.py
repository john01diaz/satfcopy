from nf_common_source.code.services.b_dictionary_service.objects.table_b_dictionaries import TableBDictionaries
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.bie_ids_register_to_table_name_filter import \
    filter_bie_ids_register_to_table_name
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.table_names_from_bie_ids_register_getter import \
    get_table_names_from_bie_ids_register


def check_bie_id_register_selected_types_uniqueness(
        bie_id_register: TableBDictionaries,
        bie_id_register_alias: str,
        bie_id_types: list) \
        -> None:
    bie_id_register_table_names = \
        get_table_names_from_bie_ids_register(
            bie_id_register_table_b_dictionary=bie_id_register)

    for bie_id_type \
            in bie_id_types:
        __check_bie_id_register_id_type_uniqueness(
            bie_id_register=bie_id_register,
            bie_id_register_alias=bie_id_register_alias,
            bie_id_register_table_names=bie_id_register_table_names,
            bie_id_type=bie_id_type)


def __check_bie_id_register_id_type_uniqueness(
        bie_id_register: TableBDictionaries,
        bie_id_register_alias: str,
        bie_id_register_table_names: list,
        bie_id_type: str) \
        -> None:
    for bie_id_register_table_name \
            in bie_id_register_table_names:
        bie_id_register_filtered_to_table = \
            filter_bie_ids_register_to_table_name(
                table_name=bie_id_register_table_name,
                bie_id_register=bie_id_register)

        __check_bie_id_register_filtered_to_table_id_type_uniqueness(
            bie_id_register_filtered_to_table=bie_id_register_filtered_to_table,
            bie_id_register_alias=bie_id_register_alias,
            table_name=bie_id_register_table_name,
            bie_id_type=bie_id_type)


def __check_bie_id_register_filtered_to_table_id_type_uniqueness(
        bie_id_register_filtered_to_table: TableBDictionaries,
        bie_id_register_alias: str,
        table_name: str,
        bie_id_type: str) \
        -> None:
    bie_ids_of_type_in_table = \
        list(
            bie_id_register_filtered_to_table.loc[bie_id_register_filtered_to_table['bie_id_types'] == bie_id_type]['bie_ids'])

    if len(bie_ids_of_type_in_table) > 1:
        # TODO: log these as proper warnings
        log_message(
            message='WARNING - the {0} value on table {1} of the {2} bie_ids_register should be unique but it is not'.format(
                bie_id_type,
                table_name,
                bie_id_register_alias))

        log_message(
            message=' - The {0} found values are: {1}'.format(
                len(bie_ids_of_type_in_table),
                bie_ids_of_type_in_table))
