
# MAGIC %python
# MAGIC
# MAGIC
# MAGIC @udf(returnType=StringType())
# MAGIC def get_terminal_marking(col, req):
# MAGIC     if col is None:
# MAGIC         return ""
# MAGIC     # req = 0 then return count of terminals
# MAGIC     if req == 0:
# MAGIC         col_list = col.split(" ", 1)
# MAGIC         return col_list[0]
# MAGIC     # req != 0 then return terminal markings
# MAGIC     else:
# MAGIC         col_list = col.split(" ", 1)
# MAGIC         if col_list[0] == "0":
# MAGIC             return None
# MAGIC         terminals = col_list[1][1 : len(col_list[1]) - 1]
# MAGIC         terminals_list = terminals.split(",")
# MAGIC         new_terminal_list = []
# MAGIC         count = 1
# MAGIC         for terminal in terminals_list:
# MAGIC             terminal = terminal.strip()
# MAGIC             if terminal != "":
# MAGIC                 new_terminal_list.append(terminal)
# MAGIC             else:
# MAGIC                 element = "NA" + str(count)
# MAGIC                 new_terminal_list.append(element)
# MAGIC                 count += 1
# MAGIC         if len(new_terminal_list) < int(col_list[0]):
# MAGIC             n = int(col_list[0]) - len(new_terminal_list)
# MAGIC             for i in range(n):
# MAGIC                 element = "NA" + str(count)
# MAGIC                 new_terminal_list.append(element)
# MAGIC                 count += 1
# MAGIC         int_terminal_list = sorted(
# MAGIC             [int(text) for text in new_terminal_list if text.isdigit()]
# MAGIC         )
# MAGIC         string_terminal_list = sorted(
# MAGIC             [text for text in new_terminal_list if not text.isdigit()]
# MAGIC         )
# MAGIC         terminal_list = [str(i) for i in int_terminal_list] + string_terminal_list
# MAGIC         return ",".join(terminal_list)
# MAGIC
# MAGIC
# MAGIC spark.udf.register("get_terminal_marking", get_terminal_marking)

