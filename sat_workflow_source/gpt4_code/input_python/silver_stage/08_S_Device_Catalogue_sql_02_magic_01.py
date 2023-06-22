

df = spark.sql('Select * from VW_DeviceCatalogue_Extract where database_name == "R_2016R3"')
# -- MAGIC
dbutils.fs.rm('dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_DeviceCatalogue',True)
# -- MAGIC
df.write.save(
    format = 'delta'
   ,mode   = 'overwrite'
   ,path   = 'dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_DeviceCatalogue'
   ,overwriteSchema = True
)