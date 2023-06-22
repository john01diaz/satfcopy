import pandas
from nf_common_source.code.services.identification_services.b_identity_ecosystem.bie_id_creators.bie_id_sum_from_strings_creator import \
    create_bie_id_sum_from_strings
from nf_common_source.code.services.identification_services.b_identity_ecosystem.objects.bie_ids import BieIds
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from sat_workflow_source.b_code.etl_processes_wrapper.objects.bie_sub_registers import BieSubRegisters
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.bie_id_registerer import register_bie_id
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.column_name_bies_create_and_registerer import \
    create_and_register_column_name_bies
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.row_bies_registerer import register_row_bies


def register_bie_table_in_bie_sub_register(
        bie_sub_register: BieSubRegisters,
        bie_table: pandas.DataFrame,
        table_name: str) \
        -> None:
    log_message(
        'Registering table - ' + table_name)
    
    create_and_register_bie_table_rows(
        bie_sub_register=bie_sub_register,
        bie_table=bie_table,
        table_name=table_name)


def create_and_register_bie_table_rows(
        bie_sub_register: BieSubRegisters,
        bie_table: pandas.DataFrame,
        table_name: str) \
        -> None:
    table_bie_id = \
        create_bie_id_sum_from_strings(
            ['table', table_name])

    # TODO: remove when structure added to BieIds
    table_bie_id.table_name = \
        table_name

    # TODO: To be deprecated when not useful
    if not isinstance(table_bie_id, BieIds):
        pass

    register_bie_id(
        bie_sub_register=bie_sub_register,
        bie_id=table_bie_id,
        bie_id_type='bie_table_ids',
        name=table_name)

    create_and_register_column_name_bies(
        bie_sub_register=bie_sub_register,
        table_bie_id=table_bie_id,
        bie_table=bie_table,
        table_name=table_name)

    register_row_bies(
        bie_sub_register=bie_sub_register,
        table_bie_id=table_bie_id,
        bie_table=bie_table,
        table_name=table_name)
