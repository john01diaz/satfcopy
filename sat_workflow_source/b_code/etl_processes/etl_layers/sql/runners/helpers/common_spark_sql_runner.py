import os
import sys

from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from pandas import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from sat_workflow_source.b_code.etl_processes.etl_layers.python.runners.helpers.dataframe_columns_to_lowercase_renamer import \
    rename_dataframe_columns_to_lowercase
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.file_content_as_string_reader import \
    read_file_content_as_string
from sat_workflow_source.b_code.etl_processes.etl_layers.sql.runners.helpers.tables_in_sql_script_renamer import \
    rename_tables_in_sql_script


def run_common_spark_sql(
        sql_script_file_path: str,
        input_tables: dict) \
        -> DataFrame:
    renamed_input_tables = \
        dict()

    rename_dictionary = \
        dict()

    for table_name, input_table in input_tables.items():
        if '.' in table_name:
            renamed_table_name = \
                table_name.replace(
                    '.',
                    '_')

            rename_dictionary[table_name] = \
                renamed_table_name

            renamed_input_tables[renamed_table_name] = \
                input_table

            log_message(
                'Table renamed for SQL processing from ' + table_name + ' to ' + renamed_table_name)

        else:
            renamed_input_tables[table_name] = \
                input_table

    base_sql_script_as_string = \
        read_file_content_as_string(
            file_path=sql_script_file_path)

    renamed_sql_script_as_string = \
        rename_tables_in_sql_script(
            base_sql_script_as_string=base_sql_script_as_string,
            sql_table_names_mapping_dictionary=rename_dictionary)

    os.environ['PYSPARK_PYTHON'] = \
        sys.executable

    os.environ['PYSPARK_DRIVER_PYTHON'] = \
        sys.executable

    spark_session = \
        SparkSession.builder.getOrCreate()

    for table_name, table \
            in renamed_input_tables.items():
        __register_table_in_spark_session(
            spark_session=spark_session,
            table=table,
            table_name=table_name)

    output_dataframe = \
        spark_session.sql(
            sqlQuery=renamed_sql_script_as_string).toPandas()

    spark_session.stop()

    rename_dataframe_columns_to_lowercase(
        dataframe=output_dataframe)

    return \
        output_dataframe


def __register_table_in_spark_session(
        spark_session: SparkSession,
        table: DataFrame,
        table_name: str) \
        -> None:
    print('creating spark dataframe for table: ' + table_name)
    if table is None:
        table_spark_dataframe = \
            spark_session.createDataFrame(
                list(),
                StructType(list()))

    else:
        table_spark_dataframe = \
            spark_session.createDataFrame(
                table)

    print('registering table in spark session: ' + table_name)

    table_spark_dataframe.createOrReplaceTempView(
        name=table_name)
