
-- MAGIC %python
 DF=spark.sql('Select * from VW_CableSchedule where database_name in (Select * from VW_Database_names)')

 dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_CableSchedule",True)

 DF = cleansing_df(DF)

 DF.write.save(
      format = "delta"
     ,path   = "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_CableSchedule"
     ,mode   = "overwrite"
     ,overwriteSchema = True
 )
