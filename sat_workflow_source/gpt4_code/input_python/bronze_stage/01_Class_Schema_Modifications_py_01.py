
LC_Wire_function = spark.read.format("delta").load(f"dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_all_db/LC_Wire_function")
LC_Wire_function=cleaning_df_columns_with_enum_display(LC_Wire_function, "LC_Wire_function_Function_pin_Rel")

LC_Wire_function=(
    LC_Wire_function
    .withColumn('from_LC_Wire_function_Function_pin_dyn_class',col('LC_connect_pin_rel')[0]['_dyn_class'])
    .withColumn('from_LC_Wire_function_Function_pin_href',col('LC_Wire_function_Function_pin_Rel')[0]['_href'])
    .withColumn('from_LC_Wire_function_Function_pin_href',split(col('from_LC_Wire_function_Function_pin_href'),'#').getItem(1))
    .withColumn('to_LC_Wire_function_Function_pin_dyn_class',col('LC_connect_pin_rel')[1]['_dyn_class'])
    .withColumn('to_LC_Wire_function_Function_pin_href',col('LC_Wire_function_Function_pin_Rel')[1]['_href'])
    .withColumn('to_LC_Wire_function_Function_pin_href',split(col('to_LC_Wire_function_Function_pin_href'),'#').getItem(1))
    .drop('LC_Wire_function_Function_pin_Rel')
)


LC_Cable_run = spark.read.format("delta").load(f"dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_all_db/LC_Cable_run")
LC_Cable_run=cleaning_df_columns_with_enum_display(LC_Cable_run)
LC_Cable_run=(
    LC_Cable_run
    .withColumn('from_Cable_run_pt_dyn_class',col('LC_Cable_run_Cable_run_pt_Rel')[0]['_dyn_class'])
    .withColumn('from_Cable_run_pt_href',col('LC_Cable_run_Cable_run_pt_Rel')[0]['_href'])
    .withColumn('to_Cable_run_pt_dyn_class',col('LC_Cable_run_Cable_run_pt_Rel')[1]['_dyn_class'])
    .withColumn('to_Cable_run_pt_href',col('LC_Cable_run_Cable_run_pt_Rel')[1]['_href'])
    .withColumn('from_Cable_run_pt_href',split('from_Cable_run_pt_href','#').getItem(1))
    .withColumn('to_Cable_run_pt_href',split('to_Cable_run_pt_href','#').getItem(1))
    .withColumn('cable_run_pt_dyn_class',coalesce(col('from_Cable_run_pt_dyn_class'),col('to_Cable_run_pt_dyn_class'),lit('null')))
    .drop('LC_Cable_run_Cable_run_pt_Rel','from_Cable_run_pt_dyn_class','to_Cable_run_pt_dyn_class')
)


LC_Connection = spark.read.format("delta").load(f"dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_all_db/LC_Connection")
LC_Connection=cleaning_df_columns_with_enum_display(LC_Connection)
LC_Connection=(
    LC_Connection
    .withColumn('from_connection_pin_dyn_class',col('LC_connect_pin_rel')[0]['_dyn_class'])
    .withColumn('from_connection_pin_href',col('LC_connect_pin_rel')[0]['_href'])
    .withColumn('from_connection_pin_href',split(col('from_connection_pin_href'),'#').getItem(1))
    .withColumn('to_connection_pin_dyn_class',col('LC_connect_pin_rel')[1]['_dyn_class'])
    .withColumn('to_connection_pin_href',col('LC_connect_pin_rel')[1]['_href'])
    .withColumn('to_connection_pin_href',split(col('to_connection_pin_href'),'#').getItem(1))
    .drop('LC_connect_pin_rel')
)

