
-- MAGIC %python
 df=spark.sql('Select * from VW_Item_Function_Model_Extract where database_name == "R_2016R3"')

 dbutils.fs.rm('dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Item_Function_Model',True)

 df.write.save(
      format          = "delta"
     ,path            = 'dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Item_Function_Model'
     ,mode            = 'overwrite'
     ,overwriteSchema = True
 )

