
df = (
    spark.read.load(
        path   = f"dbfs:/mnt/bclearer/temp/abhishek_folder/Loaders_Picklist_Values.xlsx",
        format = "com.crealytics.spark.excel",
        header = True
    )
    .where(col('Class Name').isNotNull())
)

df = df.groupby('Class Name').agg(collect_list(col("Property Name")).alias("Properties"))

