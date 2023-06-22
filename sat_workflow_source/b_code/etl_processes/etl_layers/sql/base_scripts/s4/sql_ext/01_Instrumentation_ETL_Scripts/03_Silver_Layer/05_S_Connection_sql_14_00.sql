
-- MAGIC %python
 DF=spark.sql('Select * from VW_Connection_Final_Extract where database_name == "R_2016R3"')

 dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Connection",True)

 DF.write.save(
      format = "delta"
     ,mode   = "overwrite"
     ,path   = "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Connection"
     ,overwriteSchema = True
 )
