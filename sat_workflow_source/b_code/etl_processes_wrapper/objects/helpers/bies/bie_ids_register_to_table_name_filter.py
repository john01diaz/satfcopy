from nf_common_source.code.services.b_dictionary_service.objects.row_b_dictionaries import RowBDictionaries
from nf_common_source.code.services.b_dictionary_service.objects.table_b_dictionaries import TableBDictionaries
from nf_common_source.code.services.identification_services.b_identity_ecosystem.bie_id_creators.bie_id_sum_from_strings_creator import \
    create_bie_id_sum_from_strings
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.comparison.all_bie_ids_from_table_name_getter import \
    get_all_bie_ids_from_table_name


def filter_bie_ids_register_to_table_name(
        table_name: str,
        bie_id_register: TableBDictionaries) \
        -> TableBDictionaries:
    all_bie_ids_from_table = \
        get_all_bie_ids_from_table_name(
            table_name=table_name,
            bie_id_register_table_b_dictionary=bie_id_register)

    filtered_table_name = \
        'filtered_bie_ids_register_to_' + table_name

    bie_table_id = \
        create_bie_id_sum_from_strings(
            strings=['b_table', filtered_table_name])

    bie_id_register_filtered_to_table = \
        TableBDictionaries(
            table_name=table_name,
            bie_table_id=bie_table_id)

    for row_id, row_b_dictionary in bie_id_register.dictionary.items():
        if row_b_dictionary.dictionary['bie_ids'] in all_bie_ids_from_table:
            new_row_b_dictionary = \
                RowBDictionaries(
                    dictionary=row_b_dictionary.dictionary)

            bie_id_register_filtered_to_table.add_new_row_b_dictionary(
                bie_row_id=bie_id_register_filtered_to_table.get_next_bie_row_id(),
                row_b_dictionary=new_row_b_dictionary)

    return \
        bie_id_register_filtered_to_table
