import os
import shutil
import pandas
# Note: pip install delta-spark==2.4
from delta.pip_utils import configure_spark_with_delta_pip
from deltalake import DeltaTable, Schema
import pyspark
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, BooleanType, ArrayType


def filter_latest_parquet_file_table_load_and_registerer(
        file_configuration: list,
        output_root_folder: Folders,
        number_of_rows: int = None) \
        -> None:
    match file_configuration[1]:
        case 'parquet':
            try:
                pyarrow_table, delta_table_schema = \
                    __get_parquet_table(
                        file_configuration=file_configuration)

                __export_pyarrow_table_to_parquet(
                    output_root_folder=output_root_folder,
                    pyarrow_table=pyarrow_table,
                    delta_table_schema=delta_table_schema,
                    stage_root_folder_name=file_configuration[0].split(os.sep)[-1],
                    stage=file_configuration[2],
                    number_of_rows=number_of_rows,
                    parquet_folder_name=file_configuration[3])

            except Exception as error:
                print('*'*30 + ' ERROR: ' + file_configuration[3] + ' - ' + str(error))

        case other:
            raise \
                NotImplementedError


def __get_parquet_table(
        file_configuration: list):
    user_specific_absolute_path_to_relative_path = \
        file_configuration[0]

    relative_path = \
        file_configuration[2]

    file_name_folder = \
        file_configuration[3]

    print(
        'Running on: ' + str(relative_path) + ' - ' + str(file_name_folder))

    absolute_file_name_folder_path = \
        os.path.join(
            user_specific_absolute_path_to_relative_path,
            relative_path,
            file_name_folder)

    delta_table = \
        DeltaTable(
            absolute_file_name_folder_path)

    pyarrowtable = \
        delta_table.to_pyarrow_table().to_pandas()

    print(
        'Data was read from folder: ' + str(os.sep.join(absolute_file_name_folder_path.split(os.sep)[7:])))

    return \
        pyarrowtable, \
        delta_table.schema()


def __export_pyarrow_table_to_parquet(
        output_root_folder: Folders,
        pyarrow_table: pandas.DataFrame,
        delta_table_schema: Schema,
        stage_root_folder_name: str,
        stage: str,
        parquet_folder_name: str,
        number_of_rows: int = None):
    builder = \
        pyspark.sql.SparkSession.builder.appName("MyApp")\
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.debug.maxToStringFields", 1000)

    spark_session = \
        configure_spark_with_delta_pip(builder).getOrCreate()

    output_parquet_folder_path = \
        os.path.join(
            output_root_folder.absolute_path_string,
            stage_root_folder_name,
            stage,
            parquet_folder_name)
    
    if os.path.exists(output_parquet_folder_path):
        shutil.rmtree(
            output_parquet_folder_path)

        os.mkdir(
            output_parquet_folder_path)

    else:
        os.makedirs(
            output_parquet_folder_path)

    if number_of_rows:
        pyarrow_table = \
            pyarrow_table.head(number_of_rows)

    filtered_pyarrow_table = \
        pyarrow_table.copy()

    # # TODO: it's crashing because it cannot find the correct column type for: list/array/struct
    # new_fields_schema_struct_type_format = \
    #     __parse_schema(
    #         input_schema_as_dictionary=delta_table_schema.json())
    #
    # # TODO: code added temporary - problem here has to be fixed
    # columns_to_delete = \
    #     __get_columns_as_array_and_struct(
    #         input_schema_as_dictionary=delta_table_schema.json())
    #
    # # TODO: code added temporary - problem here has to be fixed
    # for column_to_delete \
    #         in columns_to_delete:
    #     filtered_pyarrow_table = \
    #         filtered_pyarrow_table.drop(
    #             column_to_delete,
    #             axis=1)

    # column_names_to_cast = \
    #     __get_columns_as_array_and_struct(
    #         input_schema_as_dictionary=delta_table_schema.json())

    # for column_name_to_cast \
    #         in column_names_to_cast:
    #     filtered_pyarrow_table[column_name_to_cast] = \
    #         filtered_pyarrow_table[column_name_to_cast].apply(str)

    spark_dataframe = \
        spark_session.createDataFrame(
            filtered_pyarrow_table)

    # schema=new_fields_schema_struct_type_format)

    spark_dataframe = \
        spark_dataframe.coalesce(1)

    # pyarrow_table.to_parquet(
    #     output_parquet_folder_path,
    #     engine='pyarrow',
    #     partition_cols=[],
    #     compression='snappy')

    # spark_dataframe.write.save(
    #     path=output_parquet_folder_path + os.sep + 'test',
    #     # format="delta",
    #     # mode='overwrite',
    #     overwriteSchema=True)

    spark_dataframe.write.format("delta").mode("overwrite").save(
        output_parquet_folder_path)

    print('*'*25 + 'DONE: ' + str(output_parquet_folder_path))


def __get_columns_as_array_and_struct(
        input_schema_as_dictionary: dict):
    column_names = \
        []

    for field \
            in input_schema_as_dictionary['fields']:
        field_name = \
            field['name']

        field_type = \
            field['type']

        if isinstance(field_type, dict) and field_type['type'] == 'array':
            column_names.append(
                field_name)

        elif field_type == 'struct':
            column_names.append(
                field_name)

        else:
            pass

    return \
        column_names


def __parse_schema(
        input_schema_as_dictionary: dict) \
        -> StructType:
    new_fields_schema = \
        []

    for field \
            in input_schema_as_dictionary['fields']:
        field_struct = \
            None

        field_name = \
            field['name']

        field_type = \
            field['type']
            # field['type']['type'] if type(field['type']) == dict else field['type']

        nullable = \
            field['nullable']

        metadata = \
            field['metadata']

        if field_type == 'string':
            field_data_type = \
                StringType()

            field_struct = \
                StructField(
                    field_name,
                    field_data_type,
                    nullable=nullable,
                    metadata=metadata)

        elif isinstance(field_type, dict) and field_type['type'] == 'array':
            field_struct = \
                StructField(
                    field_name,
                    ArrayType(
                        StructType(
                            [
                                StructField("_dyn_class", StringType(), True),
                                StructField("_href", StringType(), True),
                                StructField("_index", StringType(), True),
                                StructField("_stat_type", StringType(), True)
                            ]
                        )
                    ),
                    nullable=nullable,
                    metadata=metadata
                )

        elif field_type == 'integer':
            field_data_type = \
                IntegerType()

            field_struct = \
                StructField(
                    field_name,
                    field_data_type,
                    nullable=nullable,
                    metadata=metadata)

        elif field_type == 'long':
            field_data_type = \
                LongType()

            field_struct = \
                StructField(
                    field_name,
                    field_data_type,
                    nullable=nullable,
                    metadata=metadata)

        elif field_type == 'boolean':
            field_data_type = \
                BooleanType()

            field_struct = \
                StructField(
                    field_name,
                    field_data_type,
                    nullable=nullable,
                    metadata=metadata)

        elif field_type == 'struct':
            nested_schema = \
                __parse_schema(field)

            field_data_type = \
                nested_schema

            field_struct = \
                StructField(
                    field_name,
                    field_data_type,
                    nullable=nullable,
                    metadata=metadata)

        else:
            raise \
                ValueError(f"Unsupported field type: {field_type}")

        new_fields_schema.append(
            field_struct)

    new_fields_schema_struct_type_format = \
        StructType(new_fields_schema)

    return \
        new_fields_schema_struct_type_format
