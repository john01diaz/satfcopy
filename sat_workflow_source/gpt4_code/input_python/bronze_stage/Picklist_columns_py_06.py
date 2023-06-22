
sigraph_dyn_class_dict = {}
for dyn_class in class_column_list:
    df = (spark.read.format('delta')
        .load(f"dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_all_db/{dyn_class[0]}")
#         .where(col('database_name') == "R_2016R3")
         )
    
    df = cleaning_df_columns_with_enum_display(df)
    
    applogger.info(f'Class_available:-{dyn_class[0]}')
    
    for dyn_class_column in dyn_class[1]:
        if dyn_class_column in df.columns:
            df1 = df.select(
                lit(dyn_class[0]).alias('SIgraph_Class_Name'),
                lit(dyn_class_column).alias('Sigraph_Property_Name'),
                col(dyn_class_column).alias('Sigraph_Property_Value'),
                regexp_replace(trim(col(dyn_class_column))," ","").alias("Dervied Column")
            ).distinct()
        else:
            applogger.info(f'{dyn_class_column} doesnot exists in {dyn_class[0]}')
            
        column_string = dyn_class[0]+'-'+dyn_class_column
        
        sigraph_dyn_class_dict[column_string] = df1

