from nf_common_source.code.services.b_dictionary_service.objects.row_b_dictionaries import RowBDictionaries
from nf_common_source.code.services.b_dictionary_service.objects.table_b_dictionaries import TableBDictionaries


def add_bie_ids_to_comparison_table(
        comparison_b_table: TableBDictionaries,
        bie_ids: set,
        bie_id_type: str,
        bie_id_register_table_b_dictionary: TableBDictionaries,
        matching_status: str,
        register: str,
        table_name: str) \
        -> None:
    for row_id, row_b_dictionary in bie_id_register_table_b_dictionary.dictionary.items():
        if row_b_dictionary.dictionary['bie_id_types'] in bie_id_type:
            if row_b_dictionary.dictionary['bie_ids'] in bie_ids:
                row_dictionary = \
                    {
                        'bie_ids': row_b_dictionary.dictionary['bie_ids'],
                        'names': row_b_dictionary.dictionary['names'],
                        'parent_bie_ids': row_b_dictionary.dictionary['parent_bie_ids'],
                        'bie_id_types': row_b_dictionary.dictionary['bie_id_types'],
                        'matching_status': matching_status,
                        'register': register,
                        'table_name': table_name
                    }

                row_b_dictionary = \
                    RowBDictionaries(
                        dictionary=row_dictionary)

                comparison_b_table.add_new_row_b_dictionary(
                    bie_row_id=comparison_b_table.get_next_bie_row_id(),
                    row_b_dictionary=row_b_dictionary)
