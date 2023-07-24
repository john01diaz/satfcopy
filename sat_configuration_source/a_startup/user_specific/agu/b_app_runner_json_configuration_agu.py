from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from sat_configuration_source.a_startup.user_specific.agu.sat_configuration_b_app_runner_configuration_getter_agu import \
    get_sat_configuration_b_app_runner_configuration_agu
from sat_configuration_source.b_code.orchestrators.json_configurations_wrapper_orchestrator import \
    orchestrate_json_configurations_wrapper


if __name__ == '__main__':
    sat_configuration_b_app_runner_configuration = \
        get_sat_configuration_b_app_runner_configuration_agu()

    run_b_app(
        app_startup_method=orchestrate_json_configurations_wrapper,
        environment_log_level_type=sat_configuration_b_app_runner_configuration.environment_log_level_type,
        output_folder_prefix=sat_configuration_b_app_runner_configuration.output_folder_prefix,
        output_folder_suffix=sat_configuration_b_app_runner_configuration.output_folder_suffix,
        output_root_folder=sat_configuration_b_app_runner_configuration.output_root_folder,
        etl_database_configuration_file=sat_configuration_b_app_runner_configuration.etl_database_configuration_file,
        etl_json_configuration_file_name=sat_configuration_b_app_runner_configuration.etl_json_configuration_file_name,
        etl_json_configuration_filtered_file_name=sat_configuration_b_app_runner_configuration.etl_json_configuration_filtered_file_name)
