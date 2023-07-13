import pandas


if __name__ == '__main__':
    input_root_folder = \
        r'/Users/terraire/bWa/DZa/etl/collect/blob_latest/reduced_parquet_sigraph_silver_2023_07_12_13_49_24/blob-temp-anusha_folder-sigraph_silver_2023_06_27_1815/sigraph_silver/S_CableCoreCatalogue'

    parquet_table = \
        pandas.read_parquet(
            input_root_folder,
            engine='pyarrow')

    print('done')
