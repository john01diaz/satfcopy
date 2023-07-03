import numpy
import pandas
from nf_common_source.code.services.identification_services.b_identity_ecosystem.bie_id_creators.bie_id_sum_from_strings_creator import \
    create_bie_id_sum_from_strings
from nf_common_source.code.services.identification_services.b_identity_ecosystem.objects.bie_ids import BieIds


def add_bie_ids_to_table(
        table: pandas.DataFrame,
        table_name: str,
        bie_id_column_name: str,
        identifier_column_names: list) \
        -> None:
    
    table[bie_id_column_name] = \
        table.apply(
            lambda table_row: __get_bie_id(table_row, table_name, identifier_column_names),
            axis=1)


def __get_bie_id(
        table_row,
        table_name: str,
        identifier_column_names: list) \
        -> BieIds:
    bie_components = \
        list()

    for identifier_column_name in identifier_column_names:
        __add_bie_component(
            bie_components=bie_components,
            table_row=table_row,
            identifier_column_name=identifier_column_name)

    bie_id = \
        create_bie_id_sum_from_strings(
            strings=bie_components)
    
    # TODO: remove when structure added to BieIds
    bie_id.table_name = \
        table_name

    return \
        bie_id


def __add_bie_component(
        bie_components: list,
        table_row,
        identifier_column_name: str) \
        -> None:
    component = \
        table_row[identifier_column_name]

    if component is None:
        component = \
            str()

    if component is numpy.nan:
        component = \
            str()

    else:
        component = \
            str(
                component)

    bie_components.append(
        component)
