from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message

from sat_workflow_source.b_code.etl_processes_wrapper.objects.bie_sub_registers import BieSubRegisters
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.checks.bie_id_register_bie_id_uniqueness_checker import \
    check_bie_table_bie_ids_for_uniqueness


def check_bie_sub_register_sanity(
        bie_sub_register: BieSubRegisters,
        register_name: str) \
        -> None:
    log_message(
        f"Checking BIE sub-register '{register_name}'")

    for table_name, bie_table in bie_sub_register.bie_tables.items():
        check_bie_table_bie_ids_for_uniqueness(
            table_name=table_name,
            bie_table=bie_table)
