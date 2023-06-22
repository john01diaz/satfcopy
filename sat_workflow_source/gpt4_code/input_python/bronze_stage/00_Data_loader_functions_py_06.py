
def bronze_metadata_validations(
     class_name: "String"
    ,rename_class_name: "String"
    ,required_columns: "List" = None
    ,explode_columns: "List" = None
  ) -> "None":
    
    applogger.info(f"Loading {class_name} into sigraph_bronze layer")
    
    try:
        df = spark.read.load(format = "delta", path  = f"dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_all_db/{class_name}")
        
        df = cleaning_df_columns_with_enum_display(df,required_columns = required_columns)
			
        if explode_columns is not None:
            for column in explode_columns:
                df = specified_column(df, column)
        
        df.write.save(format = "delta",mode  = "overwrite",overWriteSchema = True
				,path  = f"dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_bronze/{rename_class_name}")
            
    except Exception as e:
        applogger.error(f"{class_name} not loaded into sigraph_bronze layer due to {e}")
        
    else:
        pass

