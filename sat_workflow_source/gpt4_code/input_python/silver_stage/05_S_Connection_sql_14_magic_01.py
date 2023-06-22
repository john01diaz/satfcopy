

DF=spark.sql('Select * from VW_Connection_Final_Extract where database_name == "R_2016R3"')
# -- MAGIC
dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Connection",True)
# -- MAGIC
DF.write.save(
     format = "delta"
    ,mode   = "overwrite"
    ,path   = "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Connection"
    ,overwriteSchema = True
)