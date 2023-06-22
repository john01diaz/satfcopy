from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.origin_table_types import OriginTableTypes
from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.usage_table_types import UsageTableTypes


def index_table(
        etl_processes_wrapper_registry,
        table_name: str,
        origin_table_type: OriginTableTypes,
        usage_table_type: UsageTableTypes,
        process_name) \
        -> None:
    from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_registries import \
        EtlProcessesWrapperRegistries

    if not isinstance(
            etl_processes_wrapper_registry,
            EtlProcessesWrapperRegistries):
        raise \
            TypeError

    process_table_usages = \
        etl_processes_wrapper_registry.raw_and_bie_sub_register.process_table_usages

    process_table_usages.append(
        [process_name, usage_table_type, origin_table_type, table_name])
