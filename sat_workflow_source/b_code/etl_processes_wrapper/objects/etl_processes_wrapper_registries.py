import pandas

from sat_common_source.table_readers.table_loaders.source_table_load_and_registerer import \
    load_and_register_source_table
from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.origin_table_types import OriginTableTypes
from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.usage_table_types import UsageTableTypes
from sat_workflow_source.b_code.etl_processes_wrapper.objects.b_tables import BTables
from sat_workflow_source.b_code.etl_processes_wrapper.objects.bie_comparisons_sub_registers import \
    BieComparisonsSubRegisters
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bie_comparison_sub_register_reporter import \
    report_bie_comparison_sub_register
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.bie_reporter import report_bie
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.create_sql_statements_reporter import \
    report_create_sql_statements
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.files_analyser import bieize
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.files_reporter import report_files
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.index_reporter import report_index
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.python_schema_enums_reporter import \
    report_python_schema_enums
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.table_indexer import index_table
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.raw_table_registerer import register_raw_table
from sat_workflow_source.b_code.etl_processes_wrapper.objects.raw_and_bie_sub_registers import RawAndBieSubRegisters
from sat_workflow_source.b_code.etl_processes_wrapper.objects.table_configurations import TableConfigurations


class EtlProcessesWrapperRegistries:
    def __init__(
            self,
            owning_etl_processes_wrapper_universe):
        self.owning_etl_processes_wrapper_universe = \
            owning_etl_processes_wrapper_universe

        self.raw_and_bie_sub_register = \
            RawAndBieSubRegisters(
                owning_registry=self)

        self.bie_comparisons_register = \
            BieComparisonsSubRegisters(
                owning_registry=self)

    def load_and_register_source_table(
            self,
            table_configuration: TableConfigurations) \
            -> None:
        load_and_register_source_table(
            etl_processes_wrapper_registry=self,
            table_configuration=table_configuration)

    def register_generated_and_index_output_table(
            self,
            etl_table: pandas.DataFrame,
            etl_table_name: str,
            identifier_column_names: list,
            process_name: str) \
            -> None:
        register_raw_table(
            etl_processes_wrapper_registry=self,
            table=etl_table,
            table_name=etl_table_name,
            origin_table_type=OriginTableTypes.GENERATED,
            identifier_column_names=identifier_column_names)

        index_table(
            etl_processes_wrapper_registry=self,
            table_name=etl_table_name,
            origin_table_type=OriginTableTypes.GENERATED,
            usage_table_type=UsageTableTypes.OUTPUT,
            process_name=process_name)

    def get_and_index_input_dataframe(
            self,
            table_name: str,
            process_name: str) \
            -> BTables:
        origin_table_type = \
            self.owning_etl_processes_wrapper_universe.etl_processes_wrapper_configuration.get_origin_type_for_table_in_process(
                table_name=table_name,
                process_name=process_name)

        table = \
            self.raw_and_bie_sub_register.raw_sub_register[(table_name, origin_table_type)]

        index_table(
            etl_processes_wrapper_registry=self,
            table_name=table_name,
            origin_table_type=origin_table_type,
            usage_table_type=UsageTableTypes.INPUT,
            process_name=process_name)

        return \
            table

    def bieize(
            self) \
            -> None:
        bieize(
            etl_processes_wrapper_registry=self)

    def export_report_files(
            self) \
            -> None:
        report_files(
            etl_processes_wrapper_registry=self)

        report_create_sql_statements(
            etl_processes_wrapper_registry=self)

        report_python_schema_enums(
            etl_processes_wrapper_registry=self)

        report_index(
            etl_processes_wrapper_registry=self)

        report_bie(
            etl_processes_wrapper_registry=self)

        report_bie_comparison_sub_register(
            bie_comparisons_register=self.bie_comparisons_register)
