# # Databricks notebook source
# # DBTITLE 1,Import
# import pyspark
# from pyspark.sql.functions import *
# from pyspark.sql.types import *
# from pyspark.sql.window import *
# import re
#
# # EDIT OF ORIGINAL - ADDED by AMi - 2021-06-23 11:27
# from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.Enumerations import get_enumeration_display_value
# from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.Enumerations import get_system_enumeration_display_value
#
# # COMMAND ----------
#
# # DBTITLE 1,Enumerations
# # MAGIC %run /Users/p.abhishek@shell.com/01_Instrumentation_ETL_Scripts/03_Silver_Layer/Enumerations
#
# # COMMAND ----------
#
# # DBTITLE 1,Schema for Relation Columns
# schema = ArrayType(StructType([
#     StructField("_dyn_class",StringType(),True),
#     StructField("_href",StringType(),True),
#     StructField("_index",StringType(),True),
#     StructField("_stat_type", StringType(), True)
# ]))
#
# # COMMAND ----------
#
# #explode column logic
# def specified_column(df,explode_column):
#     dyn_class=explode_column+'_dyn_class'
#     href=explode_column+'_href'
#     df=(
#         df
#         .withColumn(explode_column,explode_outer(col(explode_column)))
#         .withColumn(dyn_class,col(explode_column)['_dyn_class'])
#         .withColumn(href,col(explode_column)['_href'])
#         .withColumn(href,split(col(href),'#').getItem(1))
#         .drop(explode_column)
#     )
#     return df
#
# # COMMAND ----------
#
# def cleaning_df_columns_with_enum_display (df,explode_column = None):
#
#     columns_to_select = ["database_name", "dynamic_class", "object_identifier"]
#     columns_needed_list = ["_dyn_class","_href","BS_Instance","_enum","_VALUE","-name"]
#
#     for column_name in df.columns:
#         if any(column_name.endswith(column_needed) for column_needed in columns_needed_list):
#             columns_to_select.append(column_name)
#
#     columns_to_select = columns_to_select
#
#     df = df.select(*columns_to_select)
#
#     for column_name in df.columns:
#
#         if column_name.endswith("BS_Instance"):
#             data_type = dict(df.dtypes)[column_name]
#             if data_type == "string":
#                 df = df.withColumn(column_name, from_json(col(column_name), schema, {"ignoreNullFields" : "false"}))
#
#         if "parsed_allocation" in column_name:
#             if column_name.endswith("dyn_class") and 'remote' not in column_name:
#                 column_rename = (column_name.split("-")[-3] + column_name.split("-")[-1])
#
#             elif column_name.endswith("dyn_class") and 'remote' in column_name:
#                 column_rename = (column_name.split("-")[-4] + column_name.split("-")[-1])
#
#             elif column_name.endswith("-name"):
#                 column_rename = (column_name.split("-")[-2] +'_'+ column_name.split("-")[-1])
#
#             elif column_name.endswith("href") and 'remote' not in column_name:
#                 column_rename = (column_name.split("-")[-3] + column_name.split("-")[-1])
#
#             elif column_name.endswith("href") and 'remote' in column_name:
#                 column_rename = (column_name.split("-")[-4] + column_name.split("-")[-1])
#
#             elif column_name.endswith("enum") and 'remote' not in column_name and 'BS_String_enum' not in column_name:
#                 column_rename = (column_name.split("-")[-2] + column_name.split("-")[-1])
#
#             elif column_name.endswith("enum") and 'remote' not in column_name and 'BS_String_enum'  in column_name:
#                 column_rename = (column_name.split("-")[-4] + column_name.split("-")[-1])
#
#             elif column_name.endswith("enum") and 'remote'  in column_name:
#                 column_rename = (column_name.split("-")[-4] + column_name.split("-")[-1])
#
#             elif "BS_" in column_name.split("-")[-2]:
#                 column_rename = (column_name.split("-")[-4] + column_name.split("-")[-1])
#
#             else:
#                 column_rename = (column_name.split("-")[-2])
#
#             column_rename = column_rename.replace("_VALUE", "")
#
#             df = df.withColumnRenamed(column_name, column_rename)
#
#     for column_name in df.columns:
#         if "href" in column_name:
#             df = df.withColumn(column_name, split(col(column_name), "#").getItem(1))
#
#         if '_enum' in column_name and column_name.replace('_enum',"") in df.columns:
#             df = (
#                 df
#                   .withColumn(
#                       column_name.replace('_enum',""),
#                       get_enumeration_display_value(array([column_name,column_name.replace('_enum',"")]))
#                   )
#             )
#
#             df = (
#                 df
#                   .withColumn(
#                       column_name.replace('_enum',""),
#                       get_system_enumeration_display_value(array([column_name,column_name.replace('_enum',"")]))
#                   )
# #                 .drop(column_name)
#             )
#
#     df=specified_column(df,explode_column) if explode_column else df
#
#
#     return df
#
# # COMMAND ----------
# # EDIT OF ORIGINAL - COMMENTED OUT by AMi - 2021-06-23 11:27 - spark cannot be used atm
# # def union_multiple_child_class(dataframe,child_class_column):
# #     child_classes_dict={}
# #     child_classes_list=[row[child_class_column] for row in dataframe.select(child_class_column).distinct().collect() if row[child_class_column]]
# #     for child_class in child_classes_list:
# #         df=(spark.read.format('delta').load(f"dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_all_db/{child_class}"))
# #         df=cleaning_df_columns_with_enum_display(df)
# #         child_classes_dict[child_class]=df
# #     child_element_df=child_classes_dict[child_classes_list[0]]
# #     for child_class in child_classes_list:
# #         if child_class!=child_classes_list[0]:
# #             df=child_classes_dict[child_class]
# #         child_element_df=child_element_df.unionByName(df,allowMissingColumns=True)
# #
# #     return child_element_df
#
# # COMMAND ----------
#
# import time
# def time_it(func):
#     def wrapper(*args,**kwargs):
#         start=time.time()
#         result=func(*args,**kwargs)
#         end=time.time()
#         print(func.__name__+" took " +str((end-start)*1000) + " mill seconds")
#         return result
#     return wrapper
