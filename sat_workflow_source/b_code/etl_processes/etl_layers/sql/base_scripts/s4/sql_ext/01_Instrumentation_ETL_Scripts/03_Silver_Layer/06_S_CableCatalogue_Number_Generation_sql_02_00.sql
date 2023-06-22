
-- MAGIC %python
 df = spark.sql('Select * from VW_CatalogueNo where database_name == "R_2016R3"')

 dbutils.fs.rm('dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_CableCatalogueNumber_Master',True)

 df.write.save(
     format = 'delta'
    ,mode   = 'overwrite'
    ,path   = 'dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_CableCatalogueNumber_Master'
    ,overwriteSchema = True
 )
