

# particularly Electrical database
(
    loop_df
    .select('loop_database_name','loop_dynamic_class').distinct()
    .withColumn("Rn", count('loop_database_name').over(Window.partitionBy('loop_database_name')))
    .where((col('Rn')==1) & (col('loop_dynamic_class')=="CS_Electrical_load"))
    .display()
)
          

