from nf_common_source.code.services.b_dictionary_service.objects.table_b_dictionaries import TableBDictionaries


def get_all_bie_ids_from_table_name(
        table_name: str,
        bie_id_register_table_b_dictionary: TableBDictionaries) \
        -> set:
    all_bie_ids_from_table_name = \
        set()

    for row_id, row_b_dictionary in bie_id_register_table_b_dictionary.dictionary.items():
        if row_b_dictionary.dictionary['bie_ids'].table_name == table_name:
            all_bie_ids_from_table_name.add(
                row_b_dictionary.dictionary['bie_ids'])

    return \
        all_bie_ids_from_table_name
