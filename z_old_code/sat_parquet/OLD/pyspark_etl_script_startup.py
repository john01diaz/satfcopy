from nf_common_source.code.services.file_system_service.folder_selector import select_folder
from z_old_code.sat_parquet.OLD.b_app_runner_for_parquet_files_reader import b_app_runner_for_parquet_files_reader


if __name__ == '__main__':
    input_root_folder = \
        select_folder(
            title='Select an input root folder:')

    output_root_folder = \
        select_folder(
            title='Select an output folder:')

    b_app_runner_for_parquet_files_reader(
        input_root_folder=input_root_folder,
        output_root_folder=output_root_folder,
        export_to_access=True,
        export_to_sqlite=True,
        export_csvs_to_sqlite=True,
        export_parquet_file_to_csv=True,
        data_chunk_size=100)
