
df = (
    spark
    .read
    .format("delta")
    .load("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_bronze/CS_Layer_Loop_Loop_elements")
)

