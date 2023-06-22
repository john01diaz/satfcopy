# Databricks notebook source
# MAGIC %python
# MAGIC Looplist_picklist_crosswalk =(
# MAGIC     spark
# MAGIC    .read
# MAGIC    .format('com.crealytics.spark.excel')
# MAGIC    .option("header", "True")
# MAGIC    .load(f"dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_crosswalks/Loop_List_Cross_walk.xlsx")
# MAGIC    .withColumn("Sigraph_Value_check",upper(concat(col('Column_Name'),lit(":"), col("Sigraph_Old_Value"))))
# MAGIC    .select("Sigraph_Value_check", "New_value")
# MAGIC    )
# MAGIC Looplist_picklist_crosswalk_list=Looplist_picklist_crosswalk.collect()
# MAGIC loop_picklist_enumerations = {}  
# MAGIC for enumeration in Looplist_picklist_crosswalk_list:
# MAGIC    loop_picklist_enumerations[enumeration["Sigraph_Value_check"]] = enumeration["New_value"]
# MAGIC
# MAGIC @udf(returnType=StringType())
# MAGIC def get_loop_picklist_display_value (enumeration):
# MAGIC
# MAGIC     if enumeration[1] =='null' or enumeration[1] == None:
# MAGIC        return None
# MAGIC     else:
# MAGIC         enumeration[0] = enumeration[0].upper()
# MAGIC         enumeration[1] = enumeration[1].upper()
# MAGIC         if enumeration[0] + ":" + enumeration[1] in loop_picklist_enumerations.keys():
# MAGIC             return loop_picklist_enumerations[enumeration[0] + ":" + enumeration[1]]
# MAGIC         else:
# MAGIC             return None 
# MAGIC
# MAGIC spark.udf.register("get_loop_picklist_display_value", get_loop_picklist_display_value)

