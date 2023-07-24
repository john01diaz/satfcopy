from sat_workflow_source.b_code.etl_processes.etl_layers.common.cleansing_process_runner import run_cleansing_process
from sat_workflow_source.b_code.etl_processes.etl_layers.common.database_filter_process_runner import \
    run_database_filter_process
from sat_workflow_source.b_code.etl_processes.etl_layers.common.excel_process_runner import \
    run_excel_process
from sat_workflow_source.b_code.etl_processes.etl_layers.common.copy_process_runner import run_copy_process
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.sql_process_runner import \
    run_sql_process
from sat_workflow_source.b_code.etl_processes.etl_layers.python.runners.code_process_runner import \
    run_code_process
from sat_workflow_source.b_code.etl_processes_wrapper.helpers.data_policy_to_output_table_applier import \
    migrate_output_table_to_bclearer_data_policy


class EtlProcessRunners:
    def __init__(
            self,
            etl_processes_wrapper_universe,
            process_configuration: dict):
        code_process_name = \
            process_configuration['code_process_names']

        self.etl_processes_wrapper_universe = \
            etl_processes_wrapper_universe

        self.process_configuration = \
            process_configuration

        self.code_process_name = \
            code_process_name

    def run(
            self):
        run_configuration_dictionary = \
            self.etl_processes_wrapper_universe.etl_processes_wrapper_configuration.run_configuration_as_json_dictionary

        process_table_configurations = \
            run_configuration_dictionary['process_table_configuration']

        json_list_of_table_configurations = \
            run_configuration_dictionary['table_configurations']

        input_tables_keyed_on_table_name = \
            self.__get_input_tables_dictionary(
                process_table_configurations=process_table_configurations,
                json_list_of_table_configurations=json_list_of_table_configurations)

        output_table_configuration = \
            self.__get_output_table_configuration(
                process_table_configurations=process_table_configurations,
                json_list_of_table_configurations=json_list_of_table_configurations)

        if self.process_configuration['process_types'] == 'python':
            output_table = \
                run_code_process(
                    code_process_name=self.code_process_name,
                    input_tables=input_tables_keyed_on_table_name)

        elif self.process_configuration['process_types'] == 'sql':
            output_table = \
                run_sql_process(
                    raw_and_bie_sub_register=self.etl_processes_wrapper_universe.etl_processes_wrapper_registry.raw_and_bie_sub_register,
                    process_name=self.process_configuration['process_names'],
                    input_tables=input_tables_keyed_on_table_name)

        elif self.process_configuration['process_types'] == 'excel':
            output_table = \
                run_excel_process(
                    bie_process_id=self.process_configuration['bie_process_ids'],
                    process_table_configurations=process_table_configurations,
                    code_process_name=self.code_process_name,
                    input_tables=input_tables_keyed_on_table_name,
                    table_configurations=json_list_of_table_configurations,
                    output_table_name=output_table_configuration['table_names'])

        # TODO - Remove when db is updated
        elif self.process_configuration['process_types'] == 'parquet':
            output_table = \
                run_copy_process(
                    bie_process_id=self.process_configuration['bie_process_ids'],
                    process_table_configurations=process_table_configurations,
                    input_tables=input_tables_keyed_on_table_name,
                    table_configurations=json_list_of_table_configurations)

        elif self.process_configuration['process_types'] == 'copy':
            output_table = \
                run_copy_process(
                    bie_process_id=self.process_configuration['bie_process_ids'],
                    process_table_configurations=process_table_configurations,
                    input_tables=input_tables_keyed_on_table_name,
                    table_configurations=json_list_of_table_configurations)

        elif self.process_configuration['process_types'] == 'database_filter':
            output_table = \
                run_database_filter_process(
                    bie_process_id=self.process_configuration['bie_process_ids'],
                    process_table_configurations=process_table_configurations,
                    input_tables=input_tables_keyed_on_table_name,
                    table_configurations=json_list_of_table_configurations)

        elif self.process_configuration['process_types'] == 'cleansing':
            output_table = \
                run_cleansing_process(
                    bie_process_id=self.process_configuration['bie_process_ids'],
                    process_table_configurations=process_table_configurations,
                    input_tables=input_tables_keyed_on_table_name,
                    table_configurations=json_list_of_table_configurations)

        else:
            raise \
                NotImplemented

        cleaned_output_table = \
            migrate_output_table_to_bclearer_data_policy(
                output_table=output_table)

        self.etl_processes_wrapper_universe.register_generated_and_index_output_table(
            table=cleaned_output_table,
            table_name=output_table_configuration['table_names'],
            identifier_column_names=output_table_configuration['bie_identifying_columns'],
            process_name=self.code_process_name)

    def __get_input_tables_dictionary(
            self,
            process_table_configurations: list,
            json_list_of_table_configurations: list) \
            -> dict:
        input_table_bie_ids = \
            [
                process_table_configuration_dictionary['bie_table_ids']
                for process_table_configuration_dictionary in process_table_configurations
                if process_table_configuration_dictionary['bie_process_ids'] == self.process_configuration['bie_process_ids']
                and process_table_configuration_dictionary['table_role_types'] == 'input'
            ]

        input_table_configurations = \
            dict()

        for input_table_configuration in json_list_of_table_configurations:
            bie_table_id = \
                input_table_configuration['bie_table_ids']

            if bie_table_id in input_table_bie_ids:
                if bie_table_id not in input_table_configurations.keys():
                    input_table_configurations[bie_table_id] = \
                        input_table_configuration

        input_tables_keyed_on_table_name = \
            dict()

        for bie_table_id, input_table_configuration \
                in input_table_configurations.items():
            input_table = \
                self.etl_processes_wrapper_universe.get_and_index_input_dataframe(
                    table_name=input_table_configuration['table_names'],
                    process_name=self.code_process_name).table

            input_tables_keyed_on_table_name[input_table_configuration['table_names']] = \
                input_table

        return \
            input_tables_keyed_on_table_name

    def __get_output_table_configuration(
            self,
            process_table_configurations: list,
            json_list_of_table_configurations: list) \
            -> dict:
        output_table_bie_id = \
            [
                process_table_configuration_dictionary['bie_table_ids']
                for process_table_configuration_dictionary in process_table_configurations
                if process_table_configuration_dictionary['bie_process_ids'] == self.process_configuration['bie_process_ids']
                and process_table_configuration_dictionary['table_role_types'] == 'output'
            ][0]

        output_table_configuration = \
            [
                current_output_table_configuration
                for current_output_table_configuration in json_list_of_table_configurations
                if current_output_table_configuration['bie_table_ids'] == output_table_bie_id
            ][0]

        return \
            output_table_configuration
