
def union_multiple_child_class(dataframe,child_class_column):
    child_classes_dict={}
    child_classes_list=[row[child_class_column] for row in dataframe.select(child_class_column).distinct().collect() if row[child_class_column]]
    for child_class in child_classes_list:
        df=(spark.read.format('delta').load(f"dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_all_db/{child_class}"))
        df=cleaning_df_columns_with_enum_display(df)
        child_classes_dict[child_class]=df    
    child_element_df=child_classes_dict[child_classes_list[0]]   
    for child_class in child_classes_list:
        if child_class!=child_classes_list[0]:
            df=child_classes_dict[child_class]
        child_element_df=child_element_df.unionByName(df,allowMissingColumns=True)
    
    return child_element_df

