from nf_common_source.code.services.b_dictionary_service.objects.table_b_dictionaries import TableBDictionaries
from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
from nf_common_source.code.services.table_as_dictionary_service.table_as_dictionary_to_dataframe_converter import \
    convert_table_as_dictionary_to_dataframe

from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.comparison.all_bie_ids_from_table_name_getter import \
    get_all_bie_ids_from_table_name
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.comparison.bie_ids_to_comparison_table_adder import \
    add_bie_ids_to_comparison_table


@run_and_log_function
def compare_all_bie_ids_from_table(
        etl_processes_wrapper_registry,
        before_bie_id_register_table_b_dictionary: TableBDictionaries,
        after_bie_id_register_table_b_dictionary: TableBDictionaries,
        table_name: str) \
        -> None:
    before_bie_ids = \
        get_all_bie_ids_from_table_name(
            table_name=table_name,
            bie_id_register_table_b_dictionary=before_bie_id_register_table_b_dictionary)

    after_bie_ids = \
        get_all_bie_ids_from_table_name(
            table_name=table_name,
            bie_id_register_table_b_dictionary=after_bie_id_register_table_b_dictionary)

    bie_ids_matched = \
        before_bie_ids.intersection(
            after_bie_ids)

    before_bie_ids_not_in_after = \
        before_bie_ids.difference(
            after_bie_ids)

    after_bie_ids_not_in_before = \
        after_bie_ids.difference(
            before_bie_ids)

    from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_registries import \
        EtlProcessesWrapperRegistries

    if not isinstance(etl_processes_wrapper_registry, EtlProcessesWrapperRegistries):
        raise \
            TypeError

    comparison_tables = \
        etl_processes_wrapper_registry.bie_comparisons_register.comparison_tables

    comparison_table = \
        comparison_tables['etl_analysis_bie_ids_comparison']

    add_bie_ids_to_comparison_table(
        comparison_b_table=comparison_table,
        bie_ids=bie_ids_matched,
        bie_id_register_table_b_dictionary=before_bie_id_register_table_b_dictionary,
        matching_status='matched',
        register='before',
        table_name=table_name)

    add_bie_ids_to_comparison_table(
        comparison_b_table=comparison_table,
        bie_ids=bie_ids_matched,
        bie_id_register_table_b_dictionary=after_bie_id_register_table_b_dictionary,
        matching_status='matched',
        register='after',
        table_name=table_name)

    add_bie_ids_to_comparison_table(
        comparison_b_table=comparison_table,
        bie_ids=before_bie_ids_not_in_after,
        bie_id_register_table_b_dictionary=before_bie_id_register_table_b_dictionary,
        matching_status='unmatched',
        register='before only',
        table_name=table_name)

    add_bie_ids_to_comparison_table(
        comparison_b_table=comparison_table,
        bie_ids=after_bie_ids_not_in_before,
        bie_id_register_table_b_dictionary=after_bie_id_register_table_b_dictionary,
        matching_status='unmatched',
        register='after only',
        table_name=table_name)

    __create_summary_comparison_table(
        comparison_table_as_b_dictionary=comparison_table,
        comparison_tables=comparison_tables)


def __create_summary_comparison_table(
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

    