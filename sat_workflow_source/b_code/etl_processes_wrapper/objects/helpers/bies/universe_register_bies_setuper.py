from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message

from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.origin_table_types import OriginTableTypes
from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.usage_table_types import UsageTableTypes
from sat_workflow_source.b_code.etl_processes_wrapper.objects.bie_sub_registers import BieSubRegisters
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.table_indexer import index_table
from sat_workflow_source.b_code.etl_processes_wrapper.objects.raw_and_bie_sub_registers import RawAndBieSubRegisters
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.bie_copy_of_raw_table_creator import \
    create_bie_copy_of_raw_table
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.bie_table_in_bie_sub_register_create_and_registerer import \
    register_bie_table_in_bie_sub_register


def setup_universe_register_bies(
        etl_processes_wrapper_registry) \
        -> None:
    generated_outputs = \
        list()
    
    for process_table_usage in etl_processes_wrapper_registry.raw_and_bie_sub_register.process_table_usages:
        if process_table_usage[1] == UsageTableTypes.OUTPUT:
            generated_outputs.append(
                (process_table_usage[0], process_table_usage[2], process_table_usage[3]))

        if process_table_usage[1] == UsageTableTypes.COMPARE_INPUT:
            generated_outputs.append(
                (process_table_usage[0], process_table_usage[2], process_table_usage[3]))

    for generated_output in generated_outputs:
        __populate_bie_sub_register(
            etl_processes_wrapper_registry=etl_processes_wrapper_registry,
            process_name=generated_output[0],
            origin_table_type=generated_output[1],
            table_name=generated_output[2])


def __populate_bie_sub_register(
        etl_processes_wrapper_registry,
        process_name: str,
        origin_table_type: OriginTableTypes,
        table_name: str) \
        -> None:
    if origin_table_type == OriginTableTypes.GENERATED:
        if (table_name, OriginTableTypes.SOURCE) in etl_processes_wrapper_registry.raw_and_bie_sub_register.raw_sub_register.keys():
            __bieise_tables(
                etl_processes_wrapper_registry=etl_processes_wrapper_registry,
                process_name=process_name,
                table_name=table_name)

        else:
            message = \
                'WARNING - table not BIEd - no source found for generated table - ' + table_name + ' '

            log_message(
                message=message)

        return

    if origin_table_type == OriginTableTypes.SOURCE_AFTER:
        __populate_bie_sub_register_by_origin_table_type(
            raw_and_bie_sub_register=etl_processes_wrapper_registry.raw_and_bie_sub_register,
            bie_sub_register=etl_processes_wrapper_registry.raw_and_bie_sub_register.source_after_bie_sub_register,
            origin_table_type=origin_table_type,
            table_name=table_name)

    if origin_table_type == OriginTableTypes.SOURCE_BEFORE:
        __populate_bie_sub_register_by_origin_table_type(
            raw_and_bie_sub_register=etl_processes_wrapper_registry.raw_and_bie_sub_register,
            bie_sub_register=etl_processes_wrapper_registry.raw_and_bie_sub_register.source_before_bie_sub_register,
            origin_table_type=origin_table_type,
            table_name=table_name)


def __bieise_tables(
        etl_processes_wrapper_registry,
        process_name: str,
        table_name: str):
    __populate_bie_sub_register_by_origin_table_type(
        raw_and_bie_sub_register=etl_processes_wrapper_registry.raw_and_bie_sub_register,
        bie_sub_register=etl_processes_wrapper_registry.raw_and_bie_sub_register.generated_bie_sub_register,
        origin_table_type=OriginTableTypes.GENERATED,
        table_name=table_name)

    __populate_bie_sub_register_by_origin_table_type(
        raw_and_bie_sub_register=etl_processes_wrapper_registry.raw_and_bie_sub_register,
        bie_sub_register=etl_processes_wrapper_registry.raw_and_bie_sub_register.source_bie_sub_register,
        origin_table_type=OriginTableTypes.SOURCE,
        table_name=table_name)

    index_table(
        etl_processes_wrapper_registry=etl_processes_wrapper_registry,
        table_name=table_name,
        origin_table_type=OriginTableTypes.SOURCE,
        usage_table_type=UsageTableTypes.OUTPUT,
        process_name=process_name)


def __populate_bie_sub_register_by_origin_table_type(
        raw_and_bie_sub_register: RawAndBieSubRegisters,
        bie_sub_register: BieSubRegisters,
        origin_table_type: OriginTableTypes,
        table_name: str):
    raw_table = \
        raw_and_bie_sub_register.raw_sub_register[(table_name, origin_table_type)]

    if raw_table.table is None:
        message = \
            'ERROR - table not found when setting up BIEs:' + table_name + ' ' + origin_table_type.name

        log_message(
            message=message)

        return

    bie_table = \
        create_bie_copy_of_raw_table(
            bie_sub_register=bie_sub_register,
            raw_table=raw_table,
            table_name=table_name)
    
    register_bie_table_in_bie_sub_register(
        bie_sub_register=bie_sub_register,
        bie_table=bie_table,
        table_name=table_name)
