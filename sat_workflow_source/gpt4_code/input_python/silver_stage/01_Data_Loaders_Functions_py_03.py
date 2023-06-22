
# MAGIC %python
# MAGIC
# MAGIC
# MAGIC @udf(returnType=StringType())
# MAGIC def get_process_unit(layer_Pv_base_element, layer_CS_loop_unit):
# MAGIC     if layer_Pv_base_element == None:
# MAGIC         return None
# MAGIC     list = ["[", "(", ")", "]"]
# MAGIC     if any(layer_CS_loop_unit for i in list):
# MAGIC         if re.findall("\[.*?\]", layer_CS_loop_unit):
# MAGIC             element = re.findall("\[.*?\]", layer_CS_loop_unit)[0]
# MAGIC             layer_CS_loop_unit = element[1 : len(element) - 1]
# MAGIC
# MAGIC     if any(i in layer_Pv_base_element for i in list):
# MAGIC         base_element = layer_Pv_base_element
# MAGIC         base_element = re.sub(r"[\([)\]]", "", base_element).strip()
# MAGIC         base_element = re.sub(r" ", "_", base_element)
# MAGIC         base_element = re.sub(r"-", "", base_element)
# MAGIC     else:
# MAGIC         if layer_CS_loop_unit is None:
# MAGIC             base_element = layer_Pv_base_element.strip()
# MAGIC             base_element = re.sub(r" ", "_", base_element)
# MAGIC             base_element = re.sub(r"-", "", base_element)
# MAGIC         else:
# MAGIC             base_element = layer_CS_loop_unit + "_" + layer_Pv_base_element
# MAGIC             base_element = re.sub(r" ", "_", base_element)
# MAGIC             base_element = re.sub(r"-", "", base_element)
# MAGIC     base_element = (
# MAGIC         base_element[0 : len(base_element) - 1]
# MAGIC         if base_element.endswith("_")
# MAGIC         else base_element
# MAGIC     )
# MAGIC     return base_element

