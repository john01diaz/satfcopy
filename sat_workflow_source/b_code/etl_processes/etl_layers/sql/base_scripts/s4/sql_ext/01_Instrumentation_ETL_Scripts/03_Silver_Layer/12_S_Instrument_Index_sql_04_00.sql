
-- MAGIC %python
 df = (
     spark.sql("select * from Instrument_List_Final where database_name in (Select * from VW_Database_names)")
 )

 dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Instrument_Index", True)

 df = cleansing_df(df)


 df.write.save(
     format = "delta"
    ,mode   = "overwrite"
    ,path   = "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Instrument_Index"  
    ,overwriteSchema = True
 )

