from sat_workflow_source.b_code.etl_processes.common.global_flags import GlobalFlags
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.bie_sanity_checker import check_bie_sanity
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.universe_bies_setuper import \
    setup_universe_bies
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.relevant_files_comparer import \
    compare_relevant_files


def bieize(
        etl_processes_wrapper_registry) \
        -> None:
    setup_universe_bies(
        etl_processes_wrapper_registry=etl_processes_wrapper_registry)

    __analyse_bies(
        etl_processes_wrapper_registry=etl_processes_wrapper_registry)


def __analyse_bies(
        etl_processes_wrapper_registry) \
        -> None:
    if GlobalFlags.RUN_BIEIZE_SANITY_CHECK:
        check_bie_sanity(
            etl_processes_wrapper_registry=etl_processes_wrapper_registry)

    if GlobalFlags.RUN_BIEIZE_COMPARISON:
        compare_relevant_files(
            etl_processes_wrapper_registry=etl_processes_wrapper_registry,
            before_bie_sub_register=etl_processes_wrapper_registry.raw_and_bie_sub_register.source_bie_sub_register,
            after_bie_sub_register=etl_processes_wrapper_registry.raw_and_bie_sub_register.generated_bie_sub_register)
        
        compare_relevant_files(
            etl_processes_wrapper_registry=etl_processes_wrapper_registry,
            before_bie_sub_register=etl_processes_wrapper_registry.raw_and_bie_sub_register.source_before_bie_sub_register,
            after_bie_sub_register=etl_processes_wrapper_registry.raw_and_bie_sub_register.source_after_bie_sub_register)
