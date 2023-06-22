

dbutils.fs.rm('dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Component', True)
# -- MAGIC
DF=spark.sql('Select * from VW_Component_Extract where database_name in (Select * from VW_Database_names)')
# -- MAGIC
DF.write.save(
    path  = 'dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Component'
    ,format = "delta"
    ,mode   = 'overwrite'
    ,overwriteSchema = True
)