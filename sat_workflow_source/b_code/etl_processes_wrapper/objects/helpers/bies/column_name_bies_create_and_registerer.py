import pandas
from nf_common_source.code.services.identification_services.b_identity_ecosystem.bie_id_creators.bie_id_sum_from_bie_ids_creator import \
    create_bie_id_sum_from_bie_ids
from nf_common_source.code.services.identification_services.b_identity_ecosystem.bie_id_creators.bie_id_sum_from_strings_creator import \
    create_bie_id_sum_from_strings
from nf_common_source.code.services.identification_services.b_identity_ecosystem.objects.bie_ids import BieIds

from sat_workflow_source.b_code.etl_processes_wrapper.objects.bie_sub_registers import BieSubRegisters
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.bie_id_registerer import register_bie_id


def create_and_register_column_name_bies(
        bie_sub_register: BieSubRegisters,
        table_bie_id: BieIds,
        bie_table: pandas.DataFrame,
        table_name: str) \
        -> None:
    bie_column_name_ids = \
        []

    for column_name in list(bie_table.columns):
        __register_column_name_bie(
            bie_sub_register=bie_sub_register,
            table_bie_id=table_bie_id,
            table_name=table_name,
            column_name=column_name,
            bie_column_name_ids=bie_column_name_ids)

    bie_table_sum_column_name_id = \
        create_bie_id_sum_from_bie_ids(
            bie_ids=bie_column_name_ids)
    
    # TODO: remove when structure added to BieIds
    bie_table_sum_column_name_id.table_name = \
        table_name

    register_bie_id(
        bie_sub_register=bie_sub_register,
        bie_id=bie_table_sum_column_name_id,
        bie_id_type='bie_table_sum_column_name_ids',
        name='column_sum_name_id::' + table_name,
        parent_bie_id=table_bie_id)


def __register_column_name_bie(
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
