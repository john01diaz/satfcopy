from nf_common_source.code.services.identification_services.b_identity_ecosystem.bie_id_creators.bie_id_sum_from_strings_creator import create_bie_id_sum_from_strings
from nf_common_source.code.services.identification_services.b_identity_ecosystem.objects.bie_ids import BieIds
from sat_workflow_source.b_code.etl_processes_wrapper.objects.bie_sub_registers import BieSubRegisters
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.bie_id_registerer import register_bie_id


def create_and_register_column_name_bie(
        bie_sub_register: BieSubRegisters,
        table_bie_id: BieIds,
        table_name: str,
        column_name: str,
        bie_column_name_ids: list):
    column_name_bie_id = \
        create_bie_id_sum_from_strings(
            ['column_name', table_name, column_name])
    
    # TODO: remove when structure added to BieIds
    column_name_bie_id.table_name = \
        table_name

    # TODO: To be deprecated when not useful
    if not isinstance(column_name_bie_id, BieIds):
        pass

    register_bie_id(
        bie_sub_register=bie_sub_register,
        bie_id=column_name_bie_id,
        bie_id_type='bie_column_name_ids',
        name=table_name + '::' + column_name,
        parent_bie_id=table_bie_id)

    bie_column_name_ids.append(
        column_name_bie_id)
