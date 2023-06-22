

df1=spark.sql('Select * from VW_Cable_Catalogue_Extract where database_name == "R_2016R3"')
# -- MAGIC
dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_CableCatalogue",True)
# -- MAGIC
df1.write.save(
    format = "delta"
   ,mode   = "overwrite"
   ,path   = 'dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_CableCatalogue'
)

