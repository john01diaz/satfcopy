from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_c08_s_device_catalogue_dataframe_creator import \
    create_silver_08_s_device_catalogue_dataframe
from sat_workflow_source.b_code.etl_processes.etl_layers.common.etl_process_runners import EtlProcessRunners
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses


class SilverC08DeviceCatalogueSql0100Runners(
        EtlProcessRunners):
    def __init__(
            self,
            etl_processes_wrapper_universe: EtlProcessesWrapperUniverses):
        super().__init__(
            etl_processes_wrapper_universe=etl_processes_wrapper_universe)

    @run_and_log_function
    def run(
            self) \
            -> None:
        process_name = \
            self.__class__.__name__

        s_itemfunction_dataframe = \
            self.etl_processes_wrapper_universe.get_and_index_input_dataframe(
                table_name='S_Itemfunction',
                process_name=process_name).table

        s_item_function_model_dataframe = \
            self.etl_processes_wrapper_universe.get_and_index_input_dataframe(
                table_name='S_Item_Function_Model',
                process_name=process_name).table

        silver_08_s_device_catalogue_dataframe_dataframe = \
            create_silver_08_s_device_catalogue_dataframe(
                s_itemfunction_dataframe=s_itemfunction_dataframe,
                s_item_function_model_dataframe=s_item_function_model_dataframe)

        self.etl_processes_wrapper_universe.register_generated_and_index_output_table(
            table=silver_08_s_device_catalogue_dataframe_dataframe,
            table_name='S_DeviceCatalogue',
            identifier_column_names=[],
            process_name=process_name)
