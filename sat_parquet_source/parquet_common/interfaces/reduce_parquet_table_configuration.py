from enum import Enum


class ReduceParquetTableConfiguration(
        Enum):
    OPTION_PYSPARK_PANDAS = \
        'pyspark-pandas'

    OPTION_PYSPARK_PYSPARK = \
        'pyspark-pyspark'

    OPTION_PYSPARK_FASTPARQUET = \
        'pyspark-fastparquet'

    OPTION_PYSPARK_PYARROW = \
        'pyspark-pyarrow'

    OPTION_PANDAS_PYARROW = \
        'pandas-pyarrow'

    OPTION_PANDAS_FASTPARQUET = \
        'pandas-fastparquet'
