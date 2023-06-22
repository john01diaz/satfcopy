
# MAGIC %python
# MAGIC
# MAGIC import re
# MAGIC
# MAGIC
# MAGIC def sorted_nicely(l):
# MAGIC     convert = lambda text: int(text) if text.isdigit() else text
# MAGIC     alphanum_key = lambda key: [convert(c) for c in re.split("([0-9]+)", key)]
# MAGIC     return sorted(l, key=alphanum_key)
# MAGIC
# MAGIC
# MAGIC @udf(returnType=StringType())
# MAGIC def to_get_sort(col):
# MAGIC     col = sorted_nicely([i for i in col if i != ""])
# MAGIC     return ",".join(col)
# MAGIC
# MAGIC
# MAGIC spark.udf.register("to_get_sort", to_get_sort)

