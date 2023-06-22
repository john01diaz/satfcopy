
CS_loop_spez_df =(
    spark
    .read
    .format("delta")
    .load("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_all_db/CS_Loop_spez")
    # .where(col('database_name')  == "R_2016R3")
)

CS_loop_spez_df = cleaning_df_columns_with_enum_display(CS_loop_spez_df, 'CS_Loop_CS_Loop_element')



CS_loop_spez_df = (
    CS_loop_spez_df
    .withColumnRenamed('database_name' , 'loop_database_name')
    .withColumnRenamed('dynamic_class' , 'loop_dynamic_class')
    .withColumnRenamed('object_identifier' , 'loop_object_identifier')
    .withColumnRenamed('CS_comment' , 'loop_CS_comment')
    .withColumnRenamed('CS_classification_by' , 'loop_CS_classification_by')
    .withColumnRenamed('CS_classification' , 'loop_CS_classification')
    .withColumnRenamed('CS_loop_unit' , 'loop_CS_loop_unit')
    .withColumnRenamed('sap_hierarchy_level' , 'loop_sap_hierarchy_level')
    .withColumnRenamed('cs_classification_by_enum' , 'loop_cs_classification_by_enum')
    .withColumnRenamed('sap_function' , 'loop_sap_function')
    .withColumnRenamed('sap_remark' , 'loop_sap_remark')
    .withColumnRenamed('sap_short_text' , 'loop_sap_short_text')
    .withColumnRenamed('cs_classification_enum' , 'loop_cs_classification_enum')
    .withColumnRenamed('sap_transfer_status' , 'loop_sap_transfer_status')
)

