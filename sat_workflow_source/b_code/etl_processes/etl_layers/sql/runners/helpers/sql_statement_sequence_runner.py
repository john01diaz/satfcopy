from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from nf_common_source.code.services.string_service.string_by_separators_splitter import split_string_by_separators
from pandas import DataFrame
from pyspark.sql import SparkSession


def run_sql_statement_sequence(
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
                spark_session=spark_session,
                sql_statement=sql_statement,
                sql_statement_sequence_number=sql_statement_sequence_number)

    return \
        result_dataframe


def __run_sql_statement(
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

    return \
        result_dataframe
