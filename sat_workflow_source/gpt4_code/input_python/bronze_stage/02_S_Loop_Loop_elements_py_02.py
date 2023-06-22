
layer_df = spark.read.format("delta").load(
    "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_all_db/Layer"
)

layer_df = (
    cleaning_df_columns_with_enum_display(layer_df)
    .withColumnRenamed("Pv_base_element_name", "name")
    .withColumn(
        "name",
        when(
            get_process_unit("name", "CS_loop_unit") == "500_500B_VAKUUMDESTILLATION",
            "500B_VAKUUMDESTILLATION",
        ).otherwise(get_process_unit("name", "CS_loop_unit")),
    )
    .withColumn("name", upper("name"))
    .withColumn(
        "template_loop",
        when((col("name").contains("VORL")), lit("TRUE")).otherwise(lit("FALSE")),
    )
)

