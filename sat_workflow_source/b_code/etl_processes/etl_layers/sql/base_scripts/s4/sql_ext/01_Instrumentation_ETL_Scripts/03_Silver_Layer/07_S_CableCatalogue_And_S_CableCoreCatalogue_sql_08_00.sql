
-- MAGIC %python
 df2 = spark.sql("""Select * from VW_Cable_Core_Extract 
                     where database_name == "R_2016R3"
                     order by database_name,Cable_Object_Identifier,Group_Marking,Group_Marking_Sequence"""
                 )

 dbutils.fs.rm('dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_CableCoreCatalogue',True)

 df2 = cleansing_df(df2)

 df2.write.save(
     format = 'delta'
     ,mode  = 'overwrite'
     ,path  = 'dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_CableCoreCatalogue'
     ,overwriteSchema = True
 )
