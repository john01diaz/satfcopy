import pandas
import pyspark
from pyspark.sql import SparkSession


def convert_dataframe_to_spark_dataframe_if_required(
        dataframe) \
        -> pyspark.sql.dataframe.DataFrame:
    if isinstance(dataframe, pyspark.sql.dataframe.DataFrame):
        return \
            dataframe

    elif isinstance(dataframe, pandas.DataFrame):
        spark_session = \
            SparkSession.builder.getOrCreate()

        spark_dataframe = \
            spark_session.createDataFrame(dataframe)

        spark_session.stop()

        return \
            spark_dataframe

    raise \
        NotImplementedError(
            'The dataframe type is not supported.' + str(type(dataframe)))
