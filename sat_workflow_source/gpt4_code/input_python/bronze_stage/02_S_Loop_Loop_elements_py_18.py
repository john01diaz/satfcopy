
dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_bronze/Loop_elements", True)
(
    both_class_loop_elements_df
    .write
    .format("delta")
    .mode("overwrite")
    .save("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_bronze/Loop_elements")
)


