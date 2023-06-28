from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.checks.bie_sub_register_sanity_checker import \
    check_bie_sub_register_sanity


@run_and_log_function
def check_bie_sanity(
        etl_processes_wrapper_registry) \
        -> None:
    from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_registries import \
        EtlProcessesWrapperRegistries

    if not isinstance(etl_processes_wrapper_registry, EtlProcessesWrapperRegistries):
        raise \
            TypeError

    raw_and_bie_sub_register = \
        etl_processes_wrapper_registry.raw_and_bie_sub_register

    check_bie_sub_register_sanity(
        bie_sub_register=raw_and_bie_sub_register.source_bie_sub_register,
        register_name='source_bie_sub_register')

    check_bie_sub_register_sanity(
        bie_sub_register=raw_and_bie_sub_register.generated_bie_sub_register,
        register_name='generated_bie_sub_register')

    check_bie_sub_register_sanity(
        bie_sub_register=raw_and_bie_sub_register.source_before_bie_sub_register,
        register_name='source_before_bie_sub_register')

    check_bie_sub_register_sanity(
        bie_sub_register=raw_and_bie_sub_register.source_after_bie_sub_register,
        register_name='source_after_bie_sub_register')
