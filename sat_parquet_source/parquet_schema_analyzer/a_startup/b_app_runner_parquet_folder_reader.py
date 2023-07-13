from nf_common_source.code.services.b_app_runner_service.b_app_runner import run_b_app
from nf_common_source.code.services.log_environment_utility_service.common_knowledge.environment_log_level_types import \
    EnvironmentLogLevelTypes
from sat_parquet_source.parquet_schema_analyzer.a_startup.b_app_runner_parquet_folder_reader_configuration_getter import \
    get_b_app_runner_parquet_folder_reader_configuration
from sat_parquet_source.parquet_schema_analyzer.b_code.orchestrators.parquet_folder_consumer_orchestrator import \
    orchestrate_parquet_folder_consumer


if __name__ == '__main__':
    parquet_folder_reader_configuration = \
        get_b_app_runner_parquet_folder_reader_configuration()

    run_b_app(
        app_startup_method=orchestrate_parquet_folder_consumer,
        environment_log_level_type=EnvironmentLogLevelTypes.FILTERED,
        output_folder_prefix=parquet_folder_reader_configuration.output_folder_prefix,
        output_folder_suffix=str(),
        output_root_folder=parquet_folder_reader_configuration.output_root_folder,
        input_root_folder=parquet_folder_reader_configuration.input_root_folder,
        output_file_suffix=parquet_folder_reader_configuration.input_root_folder_name,
        data_chunk_size=100,
        session_name_string='PySparkParquetReader',
        export_to_access=False,
        export_to_sqlite=True,
        export_parquet_file_to_csv=False)
