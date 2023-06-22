import pandas


def create_s_loop_index_from_loop_index(
        loop_index_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    s_loop_index_table = \
        loop_index_dataframe.copy(
            deep=True)

    s_loop_index_table = \
        s_loop_index_table.loc[s_loop_index_table['loop_database_name'] == 'R_2016R3']

    column_renames = \
        {
            'loop_database_name': 'database_name',
            'loop_dynamic_class': 'dynamic_class',
            'loop_object_identifier': 'object_identifier'
        }

    s_loop_index_table = \
        s_loop_index_table.rename(
            columns=column_renames)

    return \
        s_loop_index_table


#
#
#
#
#
# -- MAGIC df = (
# -- MAGIC     spark.sql("select * from Loop_Index where loop_database_name == 'R_2016R3'")
# -- MAGIC     .withColumnRenamed('loop_database_name', 'database_name')
# -- MAGIC     .withColumnRenamed('loop_dynamic_class', 'dynamic_class')
# -- MAGIC     .withColumnRenamed('loop_object_identifier', 'loop_object_identifier')
# -- MAGIC )