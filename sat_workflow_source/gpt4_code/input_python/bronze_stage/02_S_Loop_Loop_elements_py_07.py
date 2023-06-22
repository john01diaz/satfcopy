
DM_Circuit_diagram_df = (
    spark
    .read
    .format("delta")
    .load("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_all_db/DM_Circuit_diagram")
)

DM_Circuit_diagram_df = cleaning_df_columns_with_enum_display(DM_Circuit_diagram_df)

DM_Circuit_diagram_df = (
    DM_Circuit_diagram_df
    .withColumn("DM_modification_time",col("DM_modification_time").cast("Timestamp"))
    .withColumn("rn", 
                row_number()
                .over(Window.partitionBy(col("database_name"),col("CS_Loop_DM_Document_href"))
                      .orderBy(col("DM_modification_time").desc())))
    .where(col('rn')==1)
    .select(
        "database_name"
        ,"DM_Document_Number"
        ,"CS_Loop_DM_Document_dyn_class"
        ,"CS_Loop_DM_Document_href"
    )
    .distinct()
)


