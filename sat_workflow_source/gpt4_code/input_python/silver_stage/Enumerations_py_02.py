
Looplist_picklist_crosswalk =(
    spark
   .read
   .format('com.crealytics.spark.excel')
   .option("header", "True")
   .load(f"dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_crosswalks/Loop_List_Cross_walk.xlsx")
   .withColumn("Sigraph_Value_check",upper(concat(col('Column_Name'),lit(":"), col("Sigraph_Old_Value"))))
   .select("Sigraph_Value_check", "New_value")
   )
Looplist_picklist_crosswalk_list=Looplist_picklist_crosswalk.collect()
loop_picklist_enumerations = {}  
for enumeration in Looplist_picklist_crosswalk_list:
   loop_picklist_enumerations[enumeration["Sigraph_Value_check"]] = enumeration["New_value"]

@udf(returnType=StringType())
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

spark.udf.register("get_loop_picklist_display_value", get_loop_picklist_display_value)

