from nf_common_source.code.services.identification_services.b_identity_ecosystem.bie_id_creators.bie_id_sum_from_strings_creator import create_bie_id_sum_from_strings
from nf_common_source.code.services.identification_services.b_identity_ecosystem.objects.bie_ids import BieIds
from pandas import DataFrame, Series

from sat_workflow_source.b_code.etl_processes_wrapper.objects.bie_sub_registers import BieSubRegisters
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.bie_component_adder import add_bie_component
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.bie_id_registerer import register_bie_id


def create_and_register_immutable_column_bie(
        bie_sub_register: BieSubRegisters,
        table_bie_id: BieIds,
        bie_table: DataFrame,
        table_name: str,
        column_name: str):
    column = \
        bie_table[column_name]

    immutable_column_bie_id = \
        __get_bie_id(
            column=column,
            table_name=table_name)

    register_bie_id(
        bie_sub_register=bie_sub_register,
        bie_id=immutable_column_bie_id,
        bie_id_type='bie_column_sum_cell_content_ids',
        name=table_name + '::' + column_name,
        parent_bie_id=table_bie_id)


def __get_bie_id(
        column: Series,
        table_name: str) \
        -> BieIds:
    bie_components = \
        list()

    for cell_value in column.values:
        add_bie_component(
            bie_components=bie_components,
            component=cell_value)

    bie_id = \
        create_bie_id_sum_from_strings(
            strings=bie_components)

    # TODO: remove when structure added to BieIds
    bie_id.table_name = \
        table_name

    return \
        bie_id
