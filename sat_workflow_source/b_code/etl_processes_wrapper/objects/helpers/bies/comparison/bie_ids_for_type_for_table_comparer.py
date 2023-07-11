from nf_common_source.code.services.b_dictionary_service.objects.table_b_dictionaries import TableBDictionaries
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.comparison.bie_ids_to_comparison_table_adder import \
    add_bie_ids_to_comparison_table
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.comparison.summary_comparison_table_creator import \
    create_summary_comparison_table


def compare_bie_ids_for_type_for_table(
        etl_processes_wrapper_registry,
        before_bie_ids: set,
        after_bie_ids: set,
        bie_id_type: str,
        before_bie_id_register_table_b_dictionary: TableBDictionaries,
        after_bie_id_register_table_b_dictionary: TableBDictionaries,
        table_name: str) \
        -> None:
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
        bie_id_type=bie_id_type,
        bie_id_register_table_b_dictionary=before_bie_id_register_table_b_dictionary,
        matching_status='matched',
        register='before',
        table_name=table_name)

    add_bie_ids_to_comparison_table(
        comparison_b_table=comparison_table,
        bie_ids=bie_ids_matched,
        bie_id_type=bie_id_type,
        bie_id_register_table_b_dictionary=after_bie_id_register_table_b_dictionary,
        matching_status='matched',
        register='after',
        table_name=table_name)

    add_bie_ids_to_comparison_table(
        comparison_b_table=comparison_table,
        bie_ids=before_bie_ids_not_in_after,
        bie_id_type=bie_id_type,
        bie_id_register_table_b_dictionary=before_bie_id_register_table_b_dictionary,
        matching_status='unmatched',
        register='before only',
        table_name=table_name)

    add_bie_ids_to_comparison_table(
        comparison_b_table=comparison_table,
        bie_ids=after_bie_ids_not_in_before,
        bie_id_type=bie_id_type,
        bie_id_register_table_b_dictionary=after_bie_id_register_table_b_dictionary,
        matching_status='unmatched',
        register='after only',
        table_name=table_name)

    create_summary_comparison_table(
        comparison_table_as_b_dictionary=comparison_table,
        comparison_tables=comparison_tables)
