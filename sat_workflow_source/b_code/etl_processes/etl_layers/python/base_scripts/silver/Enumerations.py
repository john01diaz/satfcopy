# EDIT OF ORIGINAL - EDIT by AMi - 2021-06-23 11:27 - @udf lines commented out


# EDIT OF ORIGINAL - ADDED by AMi - 2021-06-25
from pyspark.python.pyspark.shell import spark
from pyspark.sql.functions import upper, concat, col, lit

# Databricks notebook source
from pyspark.sql.types import StringType

# COMMAND ----------

# @udf(returnType=StringType())
def get_enumeration_display_value (enumeration):
    if enumeration[1] == None:
      return None
    if enumeration[0] == None:
      return enumeration[1]
    else:
      if enumeration[0] + ":" + enumeration[1] in enumerations.keys():
        return enumerations[enumeration[0] + ":" + enumeration[1]]
      else:
        return enumeration[1]

# @udf(returnType=StringType())
def get_system_enumeration_display_value (enumeration):
    if enumeration[1] == None:
      return None
    if enumeration[0] == None:
      return enumeration[1]
    else:
      if enumeration[0] + ":" + enumeration[1] in system_enumerations.keys():
        return system_enumerations[enumeration[0] + ":" + enumeration[1]]
      else:
        return enumeration[1]

# EDIT OF ORIGINAL - EDIT by AMi - 2021-06-23 11:27 - commented out - this is done elsewhere
sigraph_enumerations =(
     spark
    .read
    .format('csv')
    .option("delimiter", ",")
    .load(r"C:\bWa\AMi\etl\collect\blob\blob-temp-anusha_folder-sigraph_reference_files_2023_05_21_1500\sigraph_reference_files\sigraph_dyn_enums.txt")
)

# sigraph_enumerations = ()
sigraph_enumerations_list = (
    sigraph_enumerations
    .withColumnRenamed("_c0", "enum")
    .withColumnRenamed("_c1", "name")
    .withColumnRenamed("_c2", "value")
    .withColumnRenamed("_c3", "description")
    .collect()
)

sigraph_system_enumerations = (
    spark
    .read
    .format('csv')
    .option("delimiter", ",")
    .load(r"C:\bWa\AMi\etl\collect\blob\blob-temp-anusha_folder-sigraph_reference_files_2023_05_21_1500\sigraph_reference_files\sigraph_dyn_system_enums.txt")
)
sigraph_system_enumerations_list = (
    sigraph_system_enumerations
    .withColumnRenamed("_c0", "enum")
    .withColumnRenamed("_c1", "name")
    .withColumnRenamed("_c2", "value")
    .withColumnRenamed("_c3", "description")
    .collect()
)

enumerations = {}
system_enumerations = {}

for enumeration in sigraph_enumerations_list:
  enumerations[enumeration["name"]] = enumeration["value"]

for enumeration in sigraph_system_enumerations_list:
  system_enumerations[enumeration["name"]] = enumeration["value"]

# COMMAND ----------

# Looplist_picklist_crosswalk =(
#     spark
#    .read
#    .format('com.crealytics.spark.excel')
#    .option("header", "True")
#    .load(r"C:\bWa\AMi\etl\collect\blob\blob-temp-anusha_folder-sigraph_crosswalks_db_2023_05_21_1500\sigraph_crosswalks\Loop_List_Cross_walk.xlsx")
#    .withColumn("Sigraph_Value_check",upper(concat(col('Column_Name'),lit(":"), col("Sigraph_Old_Value"))))
#    .select("Sigraph_Value_check", "New_value")
#    )
Looplist_picklist_crosswalk = (
    spark
    .read
    .format('csv')
    .option("delimiter", ",")
    .option("header", "true")
    .load(r"C:\bWa\AMi\etl\collect\blob\blob-temp-anusha_folder-sigraph_crosswalks_db_2023_05_21_1500\sigraph_crosswalks\Loop_List_Cross_walk.csv")
    .withColumn("Sigraph_Value_check",upper(concat(col('Column_Name'),lit(":"), col("Sigraph_Old_Value"))))
    .select("Sigraph_Value_check", "New_value")
)
Looplist_picklist_crosswalk_list = (
    Looplist_picklist_crosswalk.collect()
)
# Looplist_picklist_crosswalk_list=Looplist_picklist_crosswalk.collect()
loop_picklist_enumerations = {}
for enumeration in Looplist_picklist_crosswalk_list:
   loop_picklist_enumerations[enumeration["Sigraph_Value_check"]] = enumeration["New_value"]

# @udf(returnType=StringType())
def get_loop_picklist_display_value (enumeration):

    if enumeration[1] =='null' or enumeration[1] == None:
       return None
    else:
        enumeration[0] = enumeration[0].upper()
        enumeration[1] = enumeration[1].upper()
        if enumeration[0] + ":" + enumeration[1] in loop_picklist_enumerations.keys():
            return loop_picklist_enumerations[enumeration[0] + ":" + enumeration[1]]
        else:
            return None

# EDIT OF ORIGINAL - EDIT by AMi - 2021-06-23 11:27 - commented out - this is done elsewhere
# spark.udf.register("get_loop_picklist_display_value", get_loop_picklist_display_value)

# COMMAND ----------

# sigraph_picklist_crosswalk =(
#      spark
#     .read
#     .format('com.crealytics.spark.excel')
#     .option("header", "True")
#     .load(r"C:\bWa\AMi\etl\collect\blob\temp-abhishek_folder-cross_walk-PickList_202306231900\PickList\Picklist_ColumnName_Values.xlsx")
#     .withColumn("Sigraph_Value_check",concat(col('PickListName'),lit(":"), col("Value")))
#     .select("Sigraph_Value_check", "Value")
#     )
sigraph_picklist_crosswalk = (
    spark
    .read
    .format('csv')
    .option("delimiter", ",")
    .option("header", "true")
    .load(r"C:\bWa\AMi\etl\collect\blob\temp-abhishek_folder-cross_walk-PickList_202306231900\PickList\Picklist_ColumnName_Values.csv")
    .withColumn("Sigraph_Value_check",concat(col('PickListName'),lit(":"), col("Value")))
    .select("Sigraph_Value_check", "Value")
)

sigraph_picklist_crosswalk_list=sigraph_picklist_crosswalk.collect()
# @udf(returnType=StringType())
def get_picklist_display_value (enumeration):
    if enumeration[1] =='null' or enumeration[1] == None:
        return None
    else:
      if enumeration[0] + ":" + enumeration[1] in picklist_enumerations.keys():
        return picklist_enumerations[enumeration[0] + ":" + enumeration[1]]
      else:
        return enumeration[1]
picklist_enumerations = {}
for enumeration in sigraph_picklist_crosswalk_list:
   picklist_enumerations[enumeration["Sigraph_Value_check"]] = enumeration["Value"]

# EDIT OF ORIGINAL - EDIT by AMi - 2021-06-23 11:27 - commented out - this is done elsewhere
# spark.udf.register("get_picklist_display_value", get_picklist_display_value)
