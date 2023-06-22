
 
DFF=spark.sql('Select * from VW_ItemFunction where database_name == "R_2016R3"')
# -- MAGIC
DFF.write.save(
    path            = 'dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_ItemFunction'
   ,format          = "delta"
   ,mode            = 'overwrite'
   ,overwriteSchema = True
)