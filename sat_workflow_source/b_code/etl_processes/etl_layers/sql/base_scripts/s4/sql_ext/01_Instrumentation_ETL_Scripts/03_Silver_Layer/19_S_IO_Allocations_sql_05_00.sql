
-- MAGIC %python
 df = spark.sql("select * from IO_Allocations where database_name in (Select * from VW_Database_names)")

 dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_IO_Allocations",True)

 df = cleansing_df(df)

 df.write.save(
     path = f"dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_IO_Allocations"
    ,mode="overwrite"
    ,overwriteSchema = True
 )
