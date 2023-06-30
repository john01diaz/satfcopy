from pyspark.sql.functions import col, split, explode_outer

from sat_workflow_source.b_code.etl_processes.etl_layers.python.helpers.dataframe_to_spark_dataframe_if_required_converter import \
    convert_dataframe_to_spark_dataframe_if_required


def specified_column(df,explode_column):
    # NOTE: AMi ADDED - so that udf can be used with pandas dataframe
    df = \
        convert_dataframe_to_spark_dataframe_if_required(
            dataframe=df)

    dyn_class=explode_column+'_dyn_class'
    href=explode_column+'_href'
    df=(
        df
        .withColumn(explode_column,explode_outer(col(explode_column)))
        .withColumn(dyn_class,col(explode_column)['_dyn_class'])
        .withColumn(href,col(explode_column)['_href'])
        .withColumn(href,split(col(href),'#').getItem(1))
        .drop(explode_column)
    )
    return df
