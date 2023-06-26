from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from pandas import DataFrame
from pyspark.sql.types import StructType, StructField, BooleanType, ByteType, ShortType, IntegerType, LongType, \
    FloatType, DoubleType, StringType, TimestampType

# TODO: There are still problems when the input parquet file has a column of type array. Need to test this to find out
#  the pandas data type corresponding to that parquet array type and map to the corresponding Spark data type.
pandas_dataframe_to_spark_types_mapping = {
    'boolean': BooleanType(),
    'int8': ByteType(),
    'int16': ShortType(),
    'int32': IntegerType(),
    'int64': LongType(),
    'uint8': ByteType(),
    'uint16': ShortType(),
    'uint32': IntegerType(),
    'uint64': LongType(),
    'float32': FloatType(),
    'float64': DoubleType(),
    'string': StringType(),
    'datetime64[ns, tz]': TimestampType(),
    'category': StringType(),  # Categorical types are represented as string in Spark
    'object': StringType()  # 'object' type in pandas often means strings
}


def get_spark_dataframe_schema_from_pandas_dataframe(
        dataframe: DataFrame,
        table_name: str) \
        -> StructType:
    log_message(
        message='Getting Spark schema from Pandas dataframe for table: ' + table_name)

    pyspark_schema_field_definitions = \
        list()

    for column_name, column_type \
            in dataframe.dtypes.items():
        pandas_data_type_name = \
            str(column_type)

        if pandas_data_type_name.startswith('datetime64[ns]'):
            pandas_data_type_name = \
                'datetime64[ns, tz]' \
                if column_type.tz is not None \
                else 'datetime64[ns]'

        pyspark_schema_field_definitions.append(
            StructField(
                column_name,
                pandas_dataframe_to_spark_types_mapping[pandas_data_type_name],
                True))

    pyspark_dataframe_schema = \
        StructType(
            pyspark_schema_field_definitions)

    return \
        pyspark_dataframe_schema
