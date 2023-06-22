
def cleaning_df_columns_with_enum_display (
     df:"DataFrame"
    ,explode_columns:"String"=None
    ,required_columns:"List"=None
    ) ->"DataFrame":
    
    columns_to_select = ["database_name", "dynamic_class", "object_identifier"]
    
    if required_columns:  
        columns_to_select.extend(required_columns)
    
    columns_to_select = columns_to_select
    
    columns_needed_list = ["_dyn_class","_href","BS_Instance","_enum","_VALUE","-name"]
    
    for column_name in df.columns:
        if any(column_name.endswith(column_needed) for column_needed in columns_needed_list):
            columns_to_select.append(column_name)
    
    df = df.select(*columns_to_select)
 
    for column_name in df.columns:
                           
        if "parsed_allocation" in column_name:  
            if column_name.endswith("dyn_class") and 'remote' not in column_name:
                column_rename = (column_name.split("-")[-3] + column_name.split("-")[-1])
            
            elif column_name.endswith("dyn_class") and 'remote' in column_name:
                column_rename = (column_name.split("-")[-4] + column_name.split("-")[-1])
            
            elif column_name.endswith("-name"):
                column_rename = (column_name.split("-")[-2] +'_'+ column_name.split("-")[-1])
                
            elif column_name.endswith("href") and 'remote' not in column_name:
                column_rename = (column_name.split("-")[-3] + column_name.split("-")[-1])
            
            elif column_name.endswith("href") and 'remote' in column_name:
                column_rename = (column_name.split("-")[-4] + column_name.split("-")[-1])
                
            elif column_name.endswith("enum") and 'remote' not in column_name and 'BS_String_enum' not in column_name:
                column_rename = (column_name.split("-")[-2] + column_name.split("-")[-1])
				
            elif column_name.endswith("enum") and 'remote' not in column_name and 'BS_String_enum'  in column_name:
                column_rename = (column_name.split("-")[-4] + column_name.split("-")[-1])
				
            elif column_name.endswith("enum") and 'remote'  in column_name:
                column_rename = (column_name.split("-")[-4] + column_name.split("-")[-1])
                
            elif "BS_" in column_name.split("-")[-2]:
                column_rename = (column_name.split("-")[-4] + column_name.split("-")[-1])
                
            elif column_name.endswith('_stat_type'):
                column_rename = (column_name.split("-")[-3] + column_name.split("-")[-1])
                
            else:
                column_rename = (column_name.split("-")[-2])

            column_rename = column_rename.replace("_VALUE", "")
            
            df = df.withColumnRenamed(column_name, column_rename)
          
    for column_name in df.columns:
        if "href" in column_name:  
            df = df.withColumn(column_name, split(col(column_name), "#").getItem(1))
            
        if '_enum' in column_name and column_name.replace('_enum',"") in df.columns:
            df = (
                df.withColumn(column_name.replace('_enum',"")
                              ,get_enumeration_display_value(array([column_name,column_name.replace('_enum',"")])))
            )
            
            df = (
                df.withColumn(column_name.replace('_enum',"")
                              ,get_system_enumeration_display_value(array([column_name,column_name.replace('_enum',"")])))
            )
    
    df=specified_column(df,explode_columns) if explode_columns else df
        
        
    return df

