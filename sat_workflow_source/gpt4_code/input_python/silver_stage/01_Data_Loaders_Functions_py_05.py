
# MAGIC %python
# MAGIC spark.udf.register("formatTag", format_tag, StringType())
# MAGIC spark.udf.register("loopelementSuffix", loop_element_id_suffix, StringType())
# MAGIC spark.udf.register("get_process_unit", get_process_unit)
# MAGIC spark.udf.register("get_enumeration_display_value", get_enumeration_display_value)

