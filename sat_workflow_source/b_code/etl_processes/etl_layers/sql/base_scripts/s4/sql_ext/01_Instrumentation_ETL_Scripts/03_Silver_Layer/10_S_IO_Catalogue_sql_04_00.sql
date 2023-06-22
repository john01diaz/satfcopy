
-- MAGIC %python
 df = spark.sql('SELECT * from VW_IO_CATALOGUE where database_name == "R_2016R3"')

 dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_IO_Catalogue",True)

 df.write.save(
      format = "delta"
     ,mode  = "overwrite"
     ,path   = "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_IO_Catalogue"
     ,overwriteSchema = True
 )
