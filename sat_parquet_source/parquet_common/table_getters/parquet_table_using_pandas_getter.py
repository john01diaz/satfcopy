import pandas
from deltalake import DeltaTable


def get_parquet_table_using_delta_lake(
        absolute_table_name_folder_path: str) \
        -> pandas.DataFrame:
    delta_table = \
        DeltaTable(
            absolute_table_name_folder_path)

    table = \
        delta_table.to_pyarrow_table().to_pandas()

    return \
        table
