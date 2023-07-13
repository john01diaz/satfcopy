from pyspark.sql.types import *


def get_sql_datatype_name_from_parquet_datatype_name(
        spark_data_type) \
        -> str:
    """
        Translates PySpark data types to SQL data types
        """
    if isinstance(spark_data_type, ByteType):
        return \
            "TINYINT"

    elif isinstance(spark_data_type, ShortType):
        return \
            "SMALLINT"

    elif isinstance(spark_data_type, IntegerType):
        return \
            "INT"

    elif isinstance(spark_data_type, LongType):
        return \
            "BIGINT"

    elif isinstance(spark_data_type, FloatType):
        return \
            "FLOAT"

    elif isinstance(spark_data_type, DoubleType):
        return \
            "DOUBLE"

    elif isinstance(spark_data_type, BooleanType):
        return \
            "BOOLEAN"

    elif isinstance(spark_data_type, StringType):
        return \
            "VARCHAR(255)"

    elif isinstance(spark_data_type, BinaryType):
        return \
            "BINARY"

    elif isinstance(spark_data_type, DateType):
        return \
            "DATE"

    elif isinstance(spark_data_type, TimestampType):
        return \
            "TIMESTAMP"

    elif isinstance(spark_data_type, DecimalType):
        return \
            f"DECIMAL({spark_data_type.precision}, {spark_data_type.scale})"

    elif isinstance(spark_data_type, ArrayType):
        return \
            f"ARRAY<{get_sql_datatype_name_from_parquet_datatype_name(spark_data_type.elementType)}>"

    elif isinstance(spark_data_type, MapType):
        return \
            f"MAP<{get_sql_datatype_name_from_parquet_datatype_name(spark_data_type.keyType)}, " \
            f"{get_sql_datatype_name_from_parquet_datatype_name(spark_data_type.valueType)}>"

    elif isinstance(spark_data_type, StructType):
        fields = \
            [f"{field.name}:{get_sql_datatype_name_from_parquet_datatype_name(field.dataType)}" for field in spark_data_type.fields]

        return \
            f"STRUCT<{','.join(fields)}>"

    else:
        return \
            "UNKNOWN"
