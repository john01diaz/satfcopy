import pyarrow.parquet as pq
from deltalake import DeltaTable


def get_parquet_file_schema_as_dictionary(
        delta_table: DeltaTable,
        parquet_file_path: str) \
        -> dict:
    # parquet_file = \
    #     pq.ParquetFile(parquet_file_path)
    #
    # parquet_file_schema = \
    #     parquet_file.schema
    #

    parquet_file_schema = \
        delta_table.schema()

    parquet_file_schema_dictionary = \
        dict()

    for index, field \
            in enumerate(parquet_file_schema.fields):
        parquet_file_schema_dictionary[index] = \
            {
                'column_names': field.name,
                'datatypes': field.type.type
            }

    # for i in range(len(parquet_file_schema)):
    #     field = \
    #         parquet_file_schema[i]
    #
    #     parquet_file_schema_dictionary['field_name'] = \
    #         field.name
    #
    #     parquet_file_schema_dictionary['logical_type'] = \
    #         field.logical_type.type

    return \
        parquet_file_schema_dictionary
