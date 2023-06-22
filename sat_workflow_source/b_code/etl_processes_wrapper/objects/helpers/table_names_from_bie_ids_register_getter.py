from nf_common_source.code.services.b_dictionary_service.objects.table_b_dictionaries import TableBDictionaries


def get_table_names_from_bie_ids_register(
        bie_id_register_table_b_dictionary: TableBDictionaries) \
        -> list:
    table_names = \
        list()

    # TODO: All strings should become enums
    for row_id, row_b_dictionary in bie_id_register_table_b_dictionary.dictionary.items():
        if row_b_dictionary.dictionary['bie_id_types'] == 'bie_table_ids':
            table_names.append(row_b_dictionary.dictionary['names'])

    return \
        table_names
