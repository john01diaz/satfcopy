

# common
(
    loop_df
    .select('loop_database_name','loop_dynamic_class').distinct()
    .withColumn("Rn", count('loop_database_name').over(Window.partitionBy('loop_database_name')))
    .where((col('Rn')==2) )
    .select('loop_database_name')
    .distinct()
    .display()
)
          

