
-- MAGIC %python

 dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Loop_Index",True)

 df = (
     spark.sql("select * from Loop_Index where loop_database_name == 'R_2016R3'")
     .withColumnRenamed('loop_database_name', 'database_name')
     .withColumnRenamed('loop_dynamic_class', 'dynamic_class')
     .withColumnRenamed('loop_object_identifier', 'object_identifier')
 )

 df.write.save(
     format = "delta"
    ,mode   = "overwrite"
    ,path   = "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Loop_Index"  
    ,overwriteSchema = True
 )
