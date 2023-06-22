# Databricks notebook source
def load_cross_walk(file,file_format,column_seperater=None):
    
    if file_format.upper() == "EXCEL":
        
        spark_format    = "com.crealytics.spark.excel"
        load_path = f"dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_crosswalks/{file}.xlsx"
        save_path = f"dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_crosswalks/{file}"
        
        df = spark.read.format(spark_format).load(load_path,header=True)
        
        for column in df.columns:
            if " " in column:
                rename_column = column.replace(" ", "_")
                df = df.withColumnRenamed(column, rename_column)
                
        df.write.save(format = "delta",mode = "overwrite", path = save_path)
        
        sql_query = f"""
        CREATE TABLE IF NOT EXISTS Sigraph_Reference.{file} 
        USING DELTA
        LOCATION "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_crosswalks/{file}"
        """

        df = spark.sql(sql_query)
        
    elif file_format.upper() == "CSV":
        
        spark_format    = "CSV"
        load_path = f"dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_crosswalks/{file}.csv"
        save_path = f"dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_crosswalks/{file}"
        
        df = spark.read.format(spark_format).load(load_path,header=True,sep = column_seperater)
        
        for column in df.columns:
            if " " in column:
                rename_column = column.replace(" ", "_")
                df = df.withColumnRenamed(column, rename_column)
                
        df.write.save(format = "delta",mode = "overwrite", path = save_path)
        
        sql_query = f"""
        CREATE TABLE IF NOT EXISTS Sigraph_Reference.{file} 
        USING DELTA
        LOCATION "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_crosswalks/{file}"
        """

        df = spark.sql(sql_query)
        
    else:
        pass

