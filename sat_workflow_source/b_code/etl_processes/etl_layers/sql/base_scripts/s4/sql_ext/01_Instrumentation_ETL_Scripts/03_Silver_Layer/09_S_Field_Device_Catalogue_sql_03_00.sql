
-- MAGIC %python
 dbutils.fs.rm('dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Field_Device_Catalogue', True)

 DF=spark.sql('Select * from VW_FieldDevice_PrepQuery where database_name == "R_2016R3"')

 DF.write.save(
     path            = 'dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Field_Device_Catalogue'
    ,format          = 'delta'
    ,mode            = 'overwrite'
   ,overwriteSchema = True
 )
