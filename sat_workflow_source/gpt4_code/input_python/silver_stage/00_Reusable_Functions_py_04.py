
def cleaning_df_columns_with_enum_display (df,explode_column = None):
    
    columns_to_select = ["database_name", "dynamic_class", "object_identifier"]
    columns_needed_list = ["_dyn_class","_href","BS_Instance","_enum","_VALUE","-name"]
    
    for column_name in df.columns:
        if any(column_name.endswith(column_needed) for column_needed in columns_needed_list):
            columns_to_select.append(column_name)
                 
    columns_to_select = columns_to_select 
    
    df = df.select(*columns_to_select)
 
    for column_name in df.columns:
                    
        if column_name.endswith("BS_Instance"):
            data_type = dict(df.dtypes)[column_name]
            if data_type == "string":
                df = df.withColumn(column_name, from_json(col(column_name), schema, {"ignoreNullFields" : "false"}))
                
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
                
            else:
                column_rename = (column_name.split("-")[-2])

            column_rename = column_rename.replace("_VALUE", "")
            
            df = df.withColumnRenamed(column_name, column_rename)
          
    for column_name in df.columns:
        if "href" in column_name:  
            df = df.withColumn(column_name, split(col(column_name), "#").getItem(1))
            
        if '_enum' in column_name and column_name.replace('_enum',"") in df.columns:
            df = (
                df
                  .withColumn(
                      column_name.replace('_enum',""),
                      get_enumeration_display_value(array([column_name,column_name.replace('_enum',"")]))
                  )
            )
            
            df = (
                df
                  .withColumn(
                      column_name.replace('_enum',""),
                      get_system_enumeration_display_value(array([column_name,column_name.replace('_enum',"")]))
                  )
#                 .drop(column_name)
            )
    
    df=specified_column(df,explode_column) if explode_column else df
        
        
    return df

