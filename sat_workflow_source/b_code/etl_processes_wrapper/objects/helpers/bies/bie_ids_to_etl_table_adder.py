import pandas
from nf_common_source.code.services.identification_services.b_identity_ecosystem.bie_id_creators.bie_id_sum_from_strings_creator import \
    create_bie_id_sum_from_strings
from nf_common_source.code.services.identification_services.b_identity_ecosystem.objects.bie_ids import BieIds
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message

from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.bie_component_adder import add_bie_component


def add_bie_ids_to_table(
        table: pandas.DataFrame,
        table_name: str,
        bie_id_column_name: str,
        identifier_column_names: list) \
        -> None:
    cleaned_identifier_column_names = \
        __clean_identifier_column_names(
            table=table,
            table_name=table_name,
            identifier_column_names=identifier_column_names)

    table[bie_id_column_name] = \
        table.apply(
            lambda table_row: __get_bie_id(table_row, table_name, cleaned_identifier_column_names),
            axis=1)


def __clean_identifier_column_names(
        table: pandas.DataFrame,
        table_name: str,
        identifier_column_names: list) \
        -> list:
    cleaned_identifier_column_names = \
        list()

    for identifier_column_name in identifier_column_names:
        if identifier_column_name in table.columns:
            cleaned_identifier_column_names.append(
                identifier_column_name)

        else:
            log_message(
                'WARNING: BIE identity column not found in table: ' + table_name + ' :: ' + identifier_column_name)

    if len(cleaned_identifier_column_names) == 0:
        cleaned_identifier_column_names = \
            table.columns.values.tolist()

        log_message(
            'WARNING: No BIE identity columns found in table (' + table_name + ') using all columns for BIE Id')

    else:
        log_message(
            'Columns used for table (' + table_name + '): ' + str(cleaned_identifier_column_names))

    return \
        cleaned_identifier_column_names


def __get_bie_id(
        table_row,
        table_name: str,
        identifier_column_names: list) \
        -> BieIds:
    bie_components = \
        list()

    for identifier_column_name in identifier_column_names:
        component = \
            table_row[identifier_column_name]

        add_bie_component(
            bie_components=bie_components,
            component=component)

    bie_id = \
        create_bie_id_sum_from_strings(
            strings=bie_components)
    
    # TODO: remove when structure added to BieIds
    bie_id.table_name = \
        table_name

    return \
        bie_id
