

DF=spark.sql('Select * from VW_Pin_Final where Terminal_Marking<>"" and Terminal_Marking not in ("0","-0","+0") and database_name == "R_2016R3"')
# -- MAGIC
# -- MAGIC
DF.write.save(
    path = 'dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Pin'
    ,format = "delta"
    ,mode   = "overwrite"
    ,overwriteSchema = True
)