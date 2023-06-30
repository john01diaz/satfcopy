from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.origin_table_types import OriginTableTypes


class TableConfigurations:
    def __init__(
            self,
            json_table_configuration: dict,
            json_list_of_process_table_configurations: list):
        self.bie_table_id = \
            json_table_configuration['bie_table_ids']

        self.table_name = \
            json_table_configuration['table_names']

        self.alternative_table_name = \
            json_table_configuration['alternative_table_names']

        self.source_extension_type = \
            json_table_configuration['source_extension_types']

        self.table_source_folder = \
            json_table_configuration['table_source_folders']

        self.table_source_relative_path = \
            json_table_configuration['table_source_relative_paths']

        self.loader_excel_sheet_name = \
            json_table_configuration['loader_excel_sheet_names']

        self.bie_identifying_columns = \
            json_table_configuration['bie_identifying_columns']

        self.expected_column_count = \
            json_table_configuration['expected_column_count']

        self.expected_row_count = \
            json_table_configuration['expected_row_count']

        self.process_table_columns_filter = \
            json_table_configuration['process_table_columns_filters']

        self.defaulted_process_table_columns = \
            json_table_configuration['defaulted_process_table_columns']

        for process_table_configuration in json_list_of_process_table_configurations:
            if process_table_configuration['bie_table_ids'] == self.bie_table_id:
                origin_table_type_string = \
                    process_table_configuration['input_origin_types']
    
                if not origin_table_type_string:
                    self.origin_table_type = \
                        OriginTableTypes.SOURCE
                
                else:
                    self.origin_table_type = \
                        [
                            input_origin_type_item for input_origin_type_item in OriginTableTypes
                            if input_origin_type_item.value == origin_table_type_string
                        ][0]
