from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from nf_common_source.code.services.string_service.string_by_separators_splitter import split_string_by_separators
from pandas import DataFrame
from pyspark.sql import SparkSession
from sat_workflow_source.b_code.etl_processes_wrapper.common_knowledge.origin_table_types import OriginTableTypes
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.python_schema_enum_from_pandas_dataframe_getter import \
    get_python_schema_enum_from_pandas_dataframe
from sat_workflow_source.b_code.etl_processes_wrapper.objects.helpers.sql_create_table_statement_from_pandas_dataframe_getter import \
    get_sql_create_table_statement_from_pandas_dataframe


def run_sql_statement_sequence(
        raw_and_bie_sub_register,
        spark_session: SparkSession,
        sql_statement_sequence_as_string: str) \
        -> DataFrame:
    sql_statement_sequence = \
        split_string_by_separators(
            string_content=sql_statement_sequence_as_string,
            separators=[';'])

    result_dataframe = \
        DataFrame()

    for sql_statement_sequence_number, sql_statement \
            in enumerate(sql_statement_sequence):
        result_dataframe = \
            __run_sql_statement(
                raw_and_bie_sub_register=raw_and_bie_sub_register,
                spark_session=spark_session,
                sql_statement=sql_statement,
                sql_statement_sequence_number=sql_statement_sequence_number)

    return \
        result_dataframe


def __run_sql_statement(
        raw_and_bie_sub_register,
        spark_session: SparkSession,
        sql_statement: str,
        sql_statement_sequence_number: int) \
        -> DataFrame:
    log_message(
        message=f'Running SQL statement: {sql_statement_sequence_number}')

    # TODO: Add defensive code to check if the sql statement is an empty string
    result_dataframe = \
        spark_session.sql(
            sqlQuery=sql_statement).toPandas()

    if sql_statement.strip().lower().startswith('create or replace temp view'):
        view_name = \
            ' '.join(sql_statement.split()).split(' ')[5]

        result_dataframe = \
            spark_session.sql(
                sqlQuery=f'SELECT * FROM {view_name}').toPandas()

        __register_output_view_schema_as_sql_and_python(
            raw_and_bie_sub_register=raw_and_bie_sub_register,
            view_name=view_name,
            dataframe=result_dataframe)

    return \
        result_dataframe


def __register_output_view_schema_as_sql_and_python(
        raw_and_bie_sub_register,
        view_name: str,
        dataframe: DataFrame) \
        -> None:
    log_message(
        message=f'Registering output view schema as SQL and Python for interim view: {view_name}')

    sql_create_statement = \
        get_sql_create_table_statement_from_pandas_dataframe(
            dataframe=dataframe,
            table_name=view_name)

    key = \
        (view_name, OriginTableTypes.GENERATED)

    raw_and_bie_sub_register.sql_create_table_statements[key] = \
        sql_create_statement

    python_schema_enum_string = \
        get_python_schema_enum_from_pandas_dataframe(
            dataframe=dataframe,
            table_name=view_name)

    raw_and_bie_sub_register.python_schema_enums[key] = \
        python_schema_enum_string
