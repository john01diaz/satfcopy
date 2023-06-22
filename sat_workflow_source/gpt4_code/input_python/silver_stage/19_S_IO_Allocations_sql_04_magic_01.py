

df = spark.sql("select * from IO_Allocations where database_name in (Select * from VW_Database_names)")
# -- MAGIC
dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_IO_Allocations",True)
# -- MAGIC
df.write.save(
    path = f"dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_IO_Allocations"
   ,mode="overwrite"
   ,overwriteSchema = True
)

