
# MAGIC %sql
# MAGIC Create
# MAGIC or replace temp view VW_Database_names As
# MAGIC Select
# MAGIC   explode(Split('R_2016R3', ',')) as Database_name

