
-- MAGIC %python
 df = spark.sql("select * from Terminations where database_name in (Select * from VW_Database_names)")

 dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Terminations",True)

 df = cleansing_df(df)

 df.write.save(
     format = "delta"
     ,path = "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Terminations"
     ,mode= "overwrite"
     ,overwriteSchema = True
 )
