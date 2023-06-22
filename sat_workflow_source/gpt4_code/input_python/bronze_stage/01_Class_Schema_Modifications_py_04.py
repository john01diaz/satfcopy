
CS_Loop_spez_df = spark.read.format("delta").load(f"dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_all_db/CS_Loop_spez")
CS_Loop_spez_df = cleaning_df_columns_with_enum_display(CS_Loop_spez_df, "CS_Loop_CS_Loop_element")


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


CS_Loop_spez_df_with_document_number = (
    CS_Loop_spez_df
    .join(DM_Circuit_diagram_df,
          (CS_Loop_spez_df.database_name == DM_Circuit_diagram_df.database_name)&
          (CS_Loop_spez_df.dynamic_class == DM_Circuit_diagram_df.CS_Loop_DM_Document_dyn_class)&
          (CS_Loop_spez_df.object_identifier == DM_Circuit_diagram_df.CS_Loop_DM_Document_href)
          ,"left"
         )
    .drop(DM_Circuit_diagram_df.database_name
          ,DM_Circuit_diagram_df.CS_Loop_DM_Document_dyn_class
          ,DM_Circuit_diagram_df.CS_Loop_DM_Document_href
         )
)

dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_bronze/CS_Loop_spez",True)

CS_Loop_spez_df_with_document_number.write.format("delta").mode("overwrite").option("mergeSchema",True).save("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_bronze/CS_Loop_spez")

