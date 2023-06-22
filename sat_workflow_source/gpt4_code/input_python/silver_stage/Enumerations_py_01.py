
@udf(returnType=StringType())
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

@udf(returnType=StringType())
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
      
sigraph_enumerations =(
     spark
    .read
    .format('csv')
    .option("delimiter", ",")
    .load(f"dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_reference_files/sigraph_dyn_enums.txt")
)
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
    .load(f"dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_reference_files/sigraph_dyn_system_enums.txt")
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

