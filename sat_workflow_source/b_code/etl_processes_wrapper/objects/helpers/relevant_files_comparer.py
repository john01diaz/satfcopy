from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
from sat_workflow_source.b_code.etl_processes_wrapper.objects.bie_sub_registers import BieSubRegisters
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bies.comparison.all_bie_ids_from_table_comparer import \
    compare_all_bie_ids_from_table
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.table_names_from_bie_ids_register_getter import \
    get_table_names_from_bie_ids_register


# TODO: compare through the bie_registers, not the actual tables. Old parameters were: etl_tables(dict),
#  analysis_tables(dict)
@run_and_log_function
def compare_relevant_files(
        etl_processes_wrapper_registry,
        before_bie_sub_register: BieSubRegisters,
        after_bie_sub_register: BieSubRegisters) \
        -> None:
    before_table_names = \
        get_table_names_from_bie_ids_register(
            bie_id_register_table_b_dictionary=before_bie_sub_register.bie_id_register_table_b_dictionary)

    after_table_names = \
        get_table_names_from_bie_ids_register(
            bie_id_register_table_b_dictionary=after_bie_sub_register.bie_id_register_table_b_dictionary)

    # TODO: Same table names should have same bie_table_ids. Would then be better to match on bie_table_ids?
    for before_table_name \
            in before_table_names:
        if before_table_name in after_table_names:
            compare_all_bie_ids_from_table(
                etl_processes_wrapper_registry=etl_processes_wrapper_registry,
                before_bie_id_register_table_b_dictionary=before_bie_sub_register.bie_id_register_table_b_dictionary,
                after_bie_id_register_table_b_dictionary=after_bie_sub_register.bie_id_register_table_b_dictionary,
                table_name=before_table_name)

        else:
            log_message(
                message='Analysis (new output) table name: {0} cannot be found in original ETL table names'.format(
                    before_table_name))
