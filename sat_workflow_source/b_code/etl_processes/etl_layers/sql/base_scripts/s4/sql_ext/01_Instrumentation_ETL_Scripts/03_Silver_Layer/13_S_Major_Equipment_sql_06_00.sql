
-- MAGIC %python
 dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Major_Equipments", True)

 DF=spark.sql('Select * from VW_Major_Equipments where database_name in (Select * from VW_Database_names)')

 DF = cleansing_df(DF)

 DF.write.save(
     path   = 'dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Major_Equipments'
     ,format = "delta"
     ,mode   = "overwrite"
     ,overwriteSchema = True
 )
