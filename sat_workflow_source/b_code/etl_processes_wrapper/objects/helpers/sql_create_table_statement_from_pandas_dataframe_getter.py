from pandas import DataFrame


def get_sql_create_table_statement_from_pandas_dataframe(
        dataframe: DataFrame,
        table_name: str) \
        -> str:
    type_mapping = {
        'int64': 'BIGINT',
        'float64': 'FLOAT',
        # 'object': 'NVARCHAR(MAX)',
        'object': 'VARCHAR(MAX)',
        'bool': 'BIT',
        'datetime64[ns]': 'DATETIME2',
        'int32': 'INT',
        'float32': 'REAL',
        'string': 'VARCHAR(MAX)'
        # 'string': 'NVARCHAR(MAX)',
        # add more if necessary
    }

    columns_sql_string = \
        ', '.join(f'{column_name} {type_mapping[str(datatype)]}'
                  for column_name, datatype in zip(dataframe.columns, dataframe.dtypes))

    sql_create_table_statement = \
        f'CREATE TABLE {table_name} ({columns_sql_string})'

    return \
        sql_create_table_statement
