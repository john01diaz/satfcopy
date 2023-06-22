from nf_common_source.code.services.b_dictionary_service.objects.table_b_dictionaries import TableBDictionaries
from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.checks.bie_id_register_bie_id_uniqueness_checker import \
    check_bie_id_register_bie_id_uniqueness
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.checks.bie_id_register_selected_types_uniqueness_checker import \
    check_bie_id_register_selected_types_uniqueness


@run_and_log_function
def check_bie_sanity(
        etl_processes_wrapper_registry) \
        -> None:
    from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_registries import \
        EtlProcessesWrapperRegistries

    if not isinstance(etl_processes_wrapper_registry, EtlProcessesWrapperRegistries):
        raise \
            TypeError

    etl_original_outputs_bie_id_register = \
        etl_processes_wrapper_registry.raw_and_bie_sub_register.source_bie_sub_register.bie_id_register_table_b_dictionary

    etl_new_outputs_bie_id_register = \
        etl_processes_wrapper_registry.raw_and_bie_sub_register.generated_bie_sub_register.bie_id_register_table_b_dictionary

    __check_bie_types_uniqueness(
        etl_original_outputs_bie_id_register=etl_original_outputs_bie_id_register,
        etl_new_outputs_bie_id_register=etl_new_outputs_bie_id_register,
        bie_id_types=['bie_table_sum_row_content_ids',
                      'bie_table_sum_row_ids'])

    __check_bie_id_uniqueness_within_registry(
        etl_original_outputs_bie_id_register=etl_original_outputs_bie_id_register,
        etl_new_outputs_bie_id_register=etl_new_outputs_bie_id_register)

    # TODO: Add check - row count per table


def __check_bie_types_uniqueness(
        etl_original_outputs_bie_id_register: TableBDictionaries,
        etl_new_outputs_bie_id_register: TableBDictionaries,
        bie_id_types: list) \
        -> None:
    check_bie_id_register_selected_types_uniqueness(
        bie_id_register=etl_original_outputs_bie_id_register,
        bie_id_register_alias='ORIGINAL',
        bie_id_types=bie_id_types)

    check_bie_id_register_selected_types_uniqueness(
        bie_id_register=etl_new_outputs_bie_id_register,
        bie_id_register_alias='NEW',
        bie_id_types=bie_id_types)


def __check_bie_id_uniqueness_within_registry(
        etl_original_outputs_bie_id_register: TableBDictionaries,
        etl_new_outputs_bie_id_register: TableBDictionaries) \
        -> None:
    check_bie_id_register_bie_id_uniqueness(
        bie_id_register=etl_original_outputs_bie_id_register,
        bie_id_register_alias='ORIGINAL')

    check_bie_id_register_bie_id_uniqueness(
        bie_id_register=etl_new_outputs_bie_id_register,
        bie_id_register_alias='NEW')
