from sat_parquet_source.parquet_common.table_getters.parquet_table_as_pandas_using_delta_lake_getter import \
    get_parquet_table_as_pandas_using_delta_lake


if __name__ == '__main__':
    input_root_folder = \
        r'/Users/terraire/bWa/DZa/etl/collect/blob_latest/' \
        r'reduced_parquet_sigraph_silver_2023_07_13_06_05_08/' \
        r'blob-temp-anusha_folder-sigraph_silver_2023_06_27_1815/sigraph_silver/S_ItemFunction'

    parquet_table_as_pandas_dataframe = \
        get_parquet_table_as_pandas_using_delta_lake(
            absolute_table_name_folder_path=input_root_folder)

    print('DONE')
