from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.universe_register_bies_setuper import \
    setup_universe_register_bies


def setup_universe_bies(
        etl_processes_wrapper_registry) \
        -> None:
    from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_registries import \
        EtlProcessesWrapperRegistries

    if not isinstance(
            etl_processes_wrapper_registry,
            EtlProcessesWrapperRegistries):
        raise \
            TypeError

    setup_universe_register_bies(
        etl_processes_wrapper_registry=etl_processes_wrapper_registry)
