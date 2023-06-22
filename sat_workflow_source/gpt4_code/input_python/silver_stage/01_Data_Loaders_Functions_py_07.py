
# MAGIC %python
# MAGIC
# MAGIC
# MAGIC @udf(returnType=ArrayType(StringType()))
# MAGIC def array_elements_sorting(column_list):
# MAGIC     column_list_1 = [i for i in column_list if i != ""]
# MAGIC     with_pin_group = []
# MAGIC     without_pin_group = []
# MAGIC     with_alpha_pins = []
# MAGIC     for i in column_list_1:
# MAGIC         if ":" in i:
# MAGIC             with_pin_group.append(i)
# MAGIC         elif i.isalpha():
# MAGIC             with_alpha_pins.append(i)
# MAGIC         else:
# MAGIC             without_pin_group.append(i)
# MAGIC
# MAGIC     without_pin_group.sort(
# MAGIC         key=lambda x: int(re.search("(\d+)", x).group(1))
# MAGIC         if (re.search("(\d+)[A-Za-z+-]*", x))
# MAGIC         else 99
# MAGIC     )
# MAGIC     final_list = without_pin_group + sorted(with_alpha_pins) + sorted(with_pin_group)
# MAGIC     return final_list
# MAGIC
# MAGIC
# MAGIC spark.udf.register("array_elements_sorting", array_elements_sorting)

