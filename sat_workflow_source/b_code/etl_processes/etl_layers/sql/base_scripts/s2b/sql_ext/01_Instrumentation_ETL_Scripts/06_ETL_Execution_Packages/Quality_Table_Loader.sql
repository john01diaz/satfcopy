





























 df = spark.sql("SELECT * FROM Sigraph_silver.UNIT_TEST_RESULTS")

 dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/Unit_Test_Results", True)

 df.write.save(
     path = "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/Unit_Test_Results",
     format = "delta",
     mode  = "overwrite"
 )

 # spark.sql("TRUNCATE TABLE Sigraph_silver.UNIT_TEST_RESULTS")
