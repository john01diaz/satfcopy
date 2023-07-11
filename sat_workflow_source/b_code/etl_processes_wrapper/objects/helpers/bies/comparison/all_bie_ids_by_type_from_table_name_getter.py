from nf_common_source.code.services.b_dictionary_service.objects.row_b_dictionaries import RowBDictionaries
from nf_common_source.code.services.b_dictionary_service.objects.table_b_dictionaries import TableBDictionaries


def get_all_bie_ids_by_type_from_table_name(
        table_name: str,
        bie_id_register_table_b_dictionary: TableBDictionaries) \
        -> dict:
    all_bie_ids_by_type_from_table_name = \
        dict()

    for row_id, row_b_dictionary in bie_id_register_table_b_dictionary.dictionary.items():
        __add_bie_id_if_required(
            row_b_dictionary,
            table_name,
            all_bie_ids_by_type_from_table_name)

    return \
        all_bie_ids_by_type_from_table_name


def __add_bie_id_if_required(
        row_b_dictionary: RowBDictionaries,
        table_name: str,
        all_bie_ids_by_type_from_table_name: dict) \
        -> None:
    if row_b_dictionary.dictionary['bie_ids'].table_name != table_name:
        return

    bie_type = \
        row_b_dictionary.dictionary['bie_id_types']

    if bie_type not in all_bie_ids_by_type_from_table_name:
        all_bie_ids_by_type_from_table_name[bie_type] = \
            set()

    all_bie_ids_by_type_from_table_name[bie_type].add(
        row_b_dictionary.dictionary['bie_ids'])
