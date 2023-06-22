
sigraph_picklist_crosswalk =(
     spark
    .read
    .format('com.crealytics.spark.excel')
    .option("header", "True")
    .load(f"dbfs:/mnt/bclearer/temp/abhishek_folder/cross_walk/PickList/Picklist_ColumnName_Values.xlsx")
    .withColumn("Sigraph_Value_check",concat(col('PickListName'),lit(":"), col("Value")))
    .select("Sigraph_Value_check", "Value")
    )

sigraph_picklist_crosswalk_list=sigraph_picklist_crosswalk.collect()
@udf(returnType=StringType())
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

spark.udf.register("get_picklist_display_value", get_picklist_display_value)