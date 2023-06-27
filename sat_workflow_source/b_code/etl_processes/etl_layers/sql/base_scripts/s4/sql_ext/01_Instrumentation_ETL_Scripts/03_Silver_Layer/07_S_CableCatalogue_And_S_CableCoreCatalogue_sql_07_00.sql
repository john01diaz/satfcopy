
-- MAGIC %python
 df1=spark.sql('Select * from VW_Cable_Catalogue_Extract where database_name == "R_2016R3"')

 dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_CableCatalogue",True)

 df1 = cleansing_df(df1)

 df1.write.save(
     format = "delta"
    ,mode   = "overwrite"
    ,path   = 'dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_CableCatalogue'
 )

