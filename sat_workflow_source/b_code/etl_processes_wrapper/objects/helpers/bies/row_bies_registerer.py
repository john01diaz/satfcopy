import pandas
from nf_common_source.code.services.identification_services.b_identity_ecosystem.bie_id_creators.bie_id_sum_from_bie_ids_creator import \
    create_bie_id_sum_from_bie_ids
from nf_common_source.code.services.identification_services.b_identity_ecosystem.objects.bie_ids import BieIds

from sat_workflow_source.b_code.etl_processes_wrapper.objects.bie_sub_registers import BieSubRegisters
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.bie_id_registerer import register_bie_id


def register_row_bies(
        bie_sub_register: BieSubRegisters,
        table_bie_id: BieIds,
        bie_table: pandas.DataFrame,
        table_name: str) \
        -> None:
    bie_row_ids = \
        []

    bie_row_content_ids = \
        []

    for row_number, bie_table_row in bie_table.iterrows():
        __register_bie_table_row_bies(
            bie_sub_register=bie_sub_register,
            bie_table_row=bie_table_row,
            table_bie_id=table_bie_id,
            table_name=table_name,
            bie_row_ids=bie_row_ids,
            bie_row_content_ids=bie_row_content_ids,
            row_number_as_sting=str(row_number))

    bie_table_sum_row_id = \
        create_bie_id_sum_from_bie_ids(
            bie_ids=bie_row_ids)

    # TODO: remove when structure added to BieIds
    bie_table_sum_row_id.table_name = \
        table_name

    register_bie_id(
        bie_sub_register=bie_sub_register,
        bie_id=bie_table_sum_row_id,
        bie_id_type='bie_table_sum_row_ids',
        name='row_sum_id::' + table_name,
        parent_bie_id=table_bie_id)

    bie_table_sum_row_content_id = \
        create_bie_id_sum_from_bie_ids(
            bie_ids=bie_row_content_ids)

    # TODO: remove when structure added to BieIds
    bie_table_sum_row_content_id.table_name = \
        table_name

    register_bie_id(
        bie_sub_register=bie_sub_register,
        bie_id=bie_table_sum_row_content_id,
        bie_id_type='bie_table_sum_row_content_ids',
        name='row_sum_content_id::' + table_name,
        parent_bie_id=table_bie_id)


def __register_bie_table_row_bies(
        bie_sub_register: BieSubRegisters,
        bie_table_row,
        table_bie_id: BieIds,
        table_name: str,
        bie_row_ids: list,
        bie_row_content_ids: list,
        row_number_as_sting: str):
    row_bie_id = \
        bie_table_row['bie_ids']

    register_bie_id(
        bie_sub_register=bie_sub_register,
        bie_id=row_bie_id,
        bie_id_type='bie_item_ids',
        name='bi::' + table_name + '::row-' + row_number_as_sting,
        parent_bie_id=table_bie_id)

    bie_row_ids.append(
        row_bie_id)

    bie_immutable_item_id = \
        bie_table_row['bie_immutable_item_ids']

    register_bie_id(
        bie_sub_register=bie_sub_register,
        bie_id=bie_immutable_item_id,
        bie_id_type='bie_immutable_item_ids',
        name='bii::' + table_name + '::row-' + row_number_as_sting,
        parent_bie_id=row_bie_id)

    bie_row_content_ids.append(
        bie_immutable_item_id)
