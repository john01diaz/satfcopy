from pyspark.shell import spark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

spark_session = SparkSession.builder.getOrCreate()

# Assuming you have a DataFrame df

df = spark_session.createDataFrame([], StructType([]))

df.createOrReplaceTempView("temp")

create_vw_database_names_query = """
Create or replace temp view VW_Database_names
As
Select 
explode(Split('R_2016R3',',')) as Database_name
"""


# Now you can run Spark SQL queries
result = spark_session.sql(create_vw_database_names_query)
result.show()

####################

import pandas as pd
import pyarrow.parquet as pq
from pandasql import sqldf

individual_parquet_file_path = \
    r'C:\bWa\OXi\etl\collect\blob\blob-temp-anusha_folder-sigraph_silver_2023_05_21_1500\sigraph_silver\S_IO_Allocations\part-00000-745b42a2-df72-4c67-9bcc-49a7a542627b-c000.snappy.parquet'

# Read parquet files into pandas dataframes
input_parquet_file_dataframe = pq.read_table(individual_parquet_file_path).to_pandas()
# data2 = pq.read_table('example2.parquet').to_pandas()

# Define a function to perform SQL queries on pandas dataframes
pysqldf = lambda q: sqldf(q, globals())

create_vw_database_names_query = """
Create or replace temp view VW_Database_names
As
Select 
explode(Split('R_2016R3',',')) as Database_name
"""

vw_database_names = \
    pysqldf(
        create_vw_database_names_query)

# Run a complex SQL query
query = """
Select Distinct
Tag_Number
,Parent_Equipment_No
,IOType
,Equipment_No
,CatalogueNo
,ChannelNumber
From input_parquet_file_dataframe
Where Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')
"""

#and database_name in (Select Database_name from VW_Database_names)

result = pysqldf(query)

print(result)
