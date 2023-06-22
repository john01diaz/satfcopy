
# MAGIC %python
# MAGIC
# MAGIC
# MAGIC def getCableCatalogue(c, t):
# MAGIC     t = t.upper()
# MAGIC     if c:
# MAGIC         c = c.upper()
# MAGIC     else:
# MAGIC         return None
# MAGIC     if t == "OAS_Coll_SH":
# MAGIC         if any(i in c for i in ["ST", "K", "C"]):
# MAGIC             return "TRUE"
# MAGIC     elif t == "GSCR":
# MAGIC         GScr = ["PIMF", "TIMF"]
# MAGIC         if any(i in c for i in GScr):
# MAGIC             return "TRUE"
# MAGIC     elif t == "ARMOURED":
# MAGIC         if "RE%" in c and any(i in c for i in ["SWA", "RG", "FG", "B", "Q"]):
# MAGIC             return "TRUE"
# MAGIC         elif "N%" in c and any(i in c for i in ["B", "G", "F", "R"]):
# MAGIC             return "TRUE"
# MAGIC         elif "A%" in c and "B" in c:
# MAGIC             return "TRUE"
# MAGIC         elif "J%" in c and "B" in c:
# MAGIC             return "TRUE"
# MAGIC         elif "S%" in c and "B" in c:
# MAGIC             return "TRUE"
# MAGIC     elif t == "ARMOURDESCRIPTION":
# MAGIC         if "N%" in c and "B" in c:
# MAGIC             return "Steel tape armouring"
# MAGIC         elif "N%" in c and "F" in c:
# MAGIC             return "Armouring of galvanized flat steel wire"
# MAGIC         elif "N%" in c and "G" in c:
# MAGIC             return "Helix of galvanized steel tape"
# MAGIC         elif "N%" in c and "R" in c:
# MAGIC             return "Armouring of galvanized round steel wires"
# MAGIC         elif "RE%" in c and "SWA" in c:
# MAGIC             return "Galvanised round steel wires"
# MAGIC         elif "RE%" in c and "RG" in c:
# MAGIC             return "Galvanised round steel wires with counter helix made of galvanised steel tape"
# MAGIC         elif "RE%" in c and "FG" in c:
# MAGIC             return "Galvanised flat steel wires with counter helix made of galvanised steel tape"
# MAGIC         elif "RE%" in c and "B" in c:
# MAGIC             return "Double layer made of galvanised steel tape"
# MAGIC         elif "RE%" in c and "Q" in c:
# MAGIC             return "Braide made of galvanised steel tape"
# MAGIC         elif (
# MAGIC             ("A%" in c and "B" in c)
# MAGIC             or ("J%" in c and "B" in c)
# MAGIC             or ("S%" in c and "B" in c)
# MAGIC         ):
# MAGIC             return "Armouring"
# MAGIC     else:
# MAGIC         return "FALSE"
# MAGIC
# MAGIC
# MAGIC spark.udf.register("getCableCatalogue", getCableCatalogue)

