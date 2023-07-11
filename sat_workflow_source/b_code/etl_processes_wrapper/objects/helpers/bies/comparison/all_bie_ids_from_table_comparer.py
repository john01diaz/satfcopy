from nf_common_source.code.services.b_dictionary_service.objects.table_b_dictionaries import TableBDictionaries
from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.comparison.all_bie_ids_by_type_from_table_name_getter import \
    get_all_bie_ids_by_type_from_table_name
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.comparison.bie_ids_for_type_for_table_comparer import \
    compare_bie_ids_for_type_for_table


@run_and_log_function
def compare_all_bie_ids_from_table(
        etl_processes_wrapper_registry,
        before_bie_id_register_table_b_dictionary: TableBDictionaries,
        after_bie_id_register_table_b_dictionary: TableBDictionaries,
        table_name: str) \
        -> None:
    before_bie_ids_by_type = \
        get_all_bie_ids_by_type_from_table_name(
            table_name=table_name,
            bie_id_register_table_b_dictionary=before_bie_id_register_table_b_dictionary)

    after_bie_ids_by_type = \
        get_all_bie_ids_by_type_from_table_name(
            table_name=table_name,
            bie_id_register_table_b_dictionary=after_bie_id_register_table_b_dictionary)

    used_bie_id_types = \
        set(before_bie_ids_by_type.keys()).union(
            set(after_bie_ids_by_type.keys()))

    for bie_id_type in used_bie_id_types:
        __compare_bie_ids_for_type_for_table(
            etl_processes_wrapper_registry=etl_processes_wrapper_registry,
            before_bie_ids_by_type=before_bie_ids_by_type,
            after_bie_ids_by_type=after_bie_ids_by_type,
            bie_id_type=bie_id_type,
            before_bie_id_register_table_b_dictionary=before_bie_id_register_table_b_dictionary,
            after_bie_id_register_table_b_dictionary=after_bie_id_register_table_b_dictionary,
            table_name=table_name)


def __compare_bie_ids_for_type_for_table(
        etl_processes_wrapper_registry,
        before_bie_ids_by_type: dict,
        after_bie_ids_by_type: dict,
        bie_id_type: str,
        before_bie_id_register_table_b_dictionary: TableBDictionaries,
        after_bie_id_register_table_b_dictionary: TableBDictionaries,
        table_name: str) \
        -> None:
    if bie_id_type in before_bie_ids_by_type.keys():
        before_bie_ids = \
            before_bie_ids_by_type[bie_id_type]

    else:
        before_bie_ids = \
            set()

    if bie_id_type in after_bie_ids_by_type.keys():
        after_bie_ids = \
            after_bie_ids_by_type[bie_id_type]

    else:
        after_bie_ids = \
            set()

    compare_bie_ids_for_type_for_table(
        etl_processes_wrapper_registry=etl_processes_wrapper_registry,
        before_bie_ids=before_bie_ids,
        after_bie_ids=after_bie_ids,
        bie_id_type=bie_id_type,
        before_bie_id_register_table_b_dictionary=before_bie_id_register_table_b_dictionary,
        after_bie_id_register_table_b_dictionary=after_bie_id_register_table_b_dictionary,
        table_name=table_name)
