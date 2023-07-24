import numpy
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from pandas import DataFrame
from pyspark.sql.types import StructType, StructField, BooleanType, ByteType, ShortType, IntegerType, LongType, \
    FloatType, DoubleType, StringType, TimestampType, ArrayType


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
    # 'object': StringType()  # 'object' type in pandas often means strings
    'object_string': StringType(),
    # TODO: Need to check if this structure is correct for all cases when the type is an array
    'object_array': ArrayType(
                StructType([
                    StructField("_dyn_class", StringType(), True),
                    StructField("_href", StringType(), True),
                    StructField("_index", StringType(), True),
                    StructField("_stat_type", StringType(), True),
                ]))
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

        if column_type == 'object':
            pandas_data_type_name = \
                __determine_data_type(
                    column_data=dataframe[column_name])

            # TODO: If the type is an array, pySpark cannot load the type numpy.ndarray into its type ArrayType.
            #  Let's try converting the numpy.ndarray fields to lists and check if that works - WORKED
            if pandas_data_type_name == 'object_array':
                dataframe[column_name] = \
                    dataframe[column_name].apply(
                        lambda column_value: column_value.tolist()
                        if column_value is not None
                        else None)

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


def __determine_data_type(
        column_data) \
        -> str:
    for value \
            in column_data:
        if value is not None and __is_array_type(value):
            return \
                'object_array'

        elif value is not None and not __is_array_type(value):
            return \
                'object_string'

    return \
        'object_string'


def __is_array_type(
        data) \
        -> bool:
    # try:
    if isinstance(data, numpy.ndarray):
        return \
            True

        # Parse the string as JSON
    #     parsed_data = \
    #         json.loads(
    #             data.replace('\n', ','))
    #
    #     # Check if it's a list and has the expected keys in each element
    #     if isinstance(parsed_data, list):
    #         expected_keys = \
    #             set(["_dyn_class", "_href", "_index", "_stat_type"])
    #
    #         for entry \
    #                 in parsed_data:
    #             if not set(entry.keys()) == expected_keys:
    #                 return \
    #                     False
    #
    #         return \
    #             True
    #
    # except json.JSONDecodeError:
    #     return \
    #         False

    return \
        False
