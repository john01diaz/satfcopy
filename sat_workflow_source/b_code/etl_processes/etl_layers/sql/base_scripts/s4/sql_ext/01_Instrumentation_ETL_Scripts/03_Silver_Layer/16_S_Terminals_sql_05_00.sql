
-- MAGIC %python
 dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Terminals", True)

 DF=spark.sql('Select * from VW_Terminals where database_name in (Select * from VW_Database_names)')

 DF.write.save(
      path        = 'dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Terminals'
     ,format      = "delta"
     ,mode        = "overwrite"
     ,overwriteSchema = True
 )
