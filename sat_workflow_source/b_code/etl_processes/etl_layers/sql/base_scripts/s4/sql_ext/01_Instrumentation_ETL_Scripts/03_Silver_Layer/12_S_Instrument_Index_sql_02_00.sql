
-- MAGIC %python
 df = (
     spark.sql("select * from Instrument_List where loop_element_database_name in (Select * from VW_Database_names)")
     .withColumnRenamed('loop_element_database_name', 'database_name')
     .withColumnRenamed('loop_element_dynamic_class', 'dynamic_class')
     .withColumnRenamed('loop_element_object_identifier','object_identifier')
 )

 dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Instrument_Index", True)

 df.write.save(
     format = "delta"
    ,mode   = "overwrite"
    ,path   = "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Instrument_Index"  
    ,overwriteSchema = True
 )

