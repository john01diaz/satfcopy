from nf_common_source.code.services.b_dictionary_service.objects.table_b_dictionaries import TableBDictionaries
from nf_common_source.code.services.table_as_dictionary_service.table_as_dictionary_to_dataframe_converter import \
    convert_table_as_dictionary_to_dataframe


def create_summary_comparison_table(
        comparison_table_as_b_dictionary: TableBDictionaries,
        comparison_tables: dict) \
        -> None:
    comparison_table_as_dictionary = \
        dict()

    for row_b_dictionary_id_b_identity, row_b_dictionary \
            in comparison_table_as_b_dictionary.dictionary.items():
        comparison_table_as_dictionary[row_b_dictionary_id_b_identity] = \
            row_b_dictionary.dictionary

    comparison_table = \
        convert_table_as_dictionary_to_dataframe(
            table_as_dictionary=comparison_table_as_dictionary)

    columns_to_group = \
        [
            'bie_id_types',
            'matching_status',
            'register',
            'table_name'
        ]

    comparison_summary_table = \
        comparison_table.groupby(
            columns_to_group).size()

    comparison_summary_table = \
        comparison_summary_table.reset_index()

    comparison_tables['etl_analysis_bie_ids_comparison_summary'] = \
        comparison_summary_table
