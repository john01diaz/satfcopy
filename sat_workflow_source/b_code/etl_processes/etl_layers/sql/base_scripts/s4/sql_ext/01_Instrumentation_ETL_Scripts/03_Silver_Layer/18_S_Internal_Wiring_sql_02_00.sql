
-- MAGIC %python
 df = spark.sql("Select * from Internal_Wiring where database_name in (Select * from VW_Database_names)")

 dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Internal_Wiring",True)

 df = cleansing_df(df)

 df.write.save(
     format = "delta"
     ,path = "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Internal_Wiring"
     ,mode="overwrite"
     ,overwriteSchema = True
 )
