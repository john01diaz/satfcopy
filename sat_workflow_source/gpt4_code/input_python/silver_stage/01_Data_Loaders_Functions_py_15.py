
# MAGIC %python
# MAGIC
# MAGIC
# MAGIC def to_get_cable_description(Description, Core_Marking_Groups, cores, EarthCore):
# MAGIC     if EarthCore.upper() == "TRUE":
# MAGIC         if (
# MAGIC             cores == "3"
# MAGIC             and "NYM-J" in Description
# MAGIC             or "NYY-J" in Description
# MAGIC             and "BK,BU" in Core_Marking_Groups
# MAGIC         ) or (
# MAGIC             cores == "3"
# MAGIC             and "H07RN-F" in Description
# MAGIC             or "H03VV-F" in Description
# MAGIC             or "H05VV-F" in Description
# MAGIC             and "BN,BU" in Core_Marking_Groups
# MAGIC         ):
# MAGIC
# MAGIC             return f"{Description} OLD"
# MAGIC
# MAGIC         elif (
# MAGIC             cores == "3"
# MAGIC             and "NYM-J" in Description
# MAGIC             or "NYY-J" in Description
# MAGIC             and "BU,BN" in Core_Marking_Groups
# MAGIC         ) or (
# MAGIC             cores == "3"
# MAGIC             and "H07RN-F" in Description
# MAGIC             or "H03VV-F" in Description
# MAGIC             or "H05VV-F" in Description
# MAGIC             and "BU,BN" in Core_Marking_Groups
# MAGIC         ):
# MAGIC
# MAGIC             return f"{Description} NEW"
# MAGIC
# MAGIC         elif (
# MAGIC             cores == "4"
# MAGIC             and "NYM-J" in Description
# MAGIC             or "NYY-J" in Description
# MAGIC             and "BK,BU,BN" in Core_Marking_Groups
# MAGIC         ) or (
# MAGIC             cores == "4"
# MAGIC             and "H07RN-F" in Description
# MAGIC             or "H03VV-F" in Description
# MAGIC             or "H05VV-F" in Description
# MAGIC             and "BK,BU,BN" in Core_Marking_Groups
# MAGIC         ):
# MAGIC
# MAGIC             return f"{Description} OLD"
# MAGIC
# MAGIC         elif (
# MAGIC             cores == "4"
# MAGIC             and "NYM-J" in Description
# MAGIC             or "NYY-J" in Description
# MAGIC             and "BN,BK,GY" in Core_Marking_Groups
# MAGIC         ) or (
# MAGIC             cores == "4"
# MAGIC             and "H07RN-F" in Description
# MAGIC             or "H03VV-F" in Description
# MAGIC             or "H05VV-F" in Description
# MAGIC             and "BN,BK,GY" in Core_Marking_Groups
# MAGIC         ):
# MAGIC
# MAGIC             return f"{Description} NEW"
# MAGIC
# MAGIC         elif (
# MAGIC             cores == "5"
# MAGIC             and "NYM-J" in Description
# MAGIC             or "NYY-J" in Description
# MAGIC             and "BK,BU,BN,BK" in Core_Marking_Groups
# MAGIC         ) or (
# MAGIC             cores == "5"
# MAGIC             and "H07RN-F" in Description
# MAGIC             or "H03VV-F" in Description
# MAGIC             or "H05VV-F" in Description
# MAGIC             and "BK,BU,BN,BK" in Core_Marking_Groups
# MAGIC         ):
# MAGIC
# MAGIC             return f"{Description} OLD"
# MAGIC
# MAGIC         elif (
# MAGIC             cores == "5"
# MAGIC             and "NYM-J" in Description
# MAGIC             or "NYY-J" in Description
# MAGIC             and "BU,BN,BK,GY" in Core_Marking_Groups
# MAGIC         ) or (
# MAGIC             cores == "5"
# MAGIC             and "H07RN-F" in Description
# MAGIC             or "H03VV-F" in Description
# MAGIC             or "H05VV-F" in Description
# MAGIC             and "BU,BN,BK,GY" in Core_Marking_Groups
# MAGIC         ):
# MAGIC
# MAGIC             return f"{Description} NEW"
# MAGIC
# MAGIC         else:
# MAGIC
# MAGIC             return f"{Description}"
# MAGIC
# MAGIC     elif (
# MAGIC         (
# MAGIC             (cores == "2")
# MAGIC             and (
# MAGIC                 "NYM-O" in Description
# MAGIC                 or "NYY-O" in Description
# MAGIC                 or "NYCWY" in Description
# MAGIC                 or "NYCY" in Description
# MAGIC             )
# MAGIC             and ("BK,BU" in Core_Marking_Groups)
# MAGIC         )
# MAGIC         or (
# MAGIC             (cores == "3")
# MAGIC             and (
# MAGIC                 "NYM-O" in Description
# MAGIC                 or "NYY-O" in Description
# MAGIC                 or "NYCWY" in Description
# MAGIC                 or "NYCY" in Description
# MAGIC             )
# MAGIC             and ("BK,BU,BN" in Core_Marking_Groups)
# MAGIC         )
# MAGIC         or (
# MAGIC             (cores == "4")
# MAGIC             and (
# MAGIC                 "NYM-O" in Description
# MAGIC                 or "NYY-O" in Description
# MAGIC                 or "NYCWY" in Description
# MAGIC                 or "NYCY" in Description
# MAGIC             )
# MAGIC             and ("BK,BU,BN,BK" in Core_Marking_Groups)
# MAGIC         )
# MAGIC         or (
# MAGIC             (cores == "5")
# MAGIC             and (
# MAGIC                 "NYM-O" in Description
# MAGIC                 or "NYY-O" in Description
# MAGIC                 or "NYCWY" in Description
# MAGIC                 or "NYCY" in Description
# MAGIC             )
# MAGIC             and ("BK,BU,BN,BK,BK" in Core_Marking_Groups)
# MAGIC         )
# MAGIC     ):
# MAGIC
# MAGIC         return f"{Description} OLD"
# MAGIC
# MAGIC     elif (
# MAGIC         (
# MAGIC             (cores == "2")
# MAGIC             and (
# MAGIC                 "NYM-O" in Description
# MAGIC                 or "NYY-O" in Description
# MAGIC                 or "NYCWY" in Description
# MAGIC                 or "NYCY" in Description
# MAGIC             )
# MAGIC             and ("BU,BN" in Core_Marking_Groups)
# MAGIC         )
# MAGIC         or (
# MAGIC             (cores == "3")
# MAGIC             and (
# MAGIC                 "NYM-O" in Description
# MAGIC                 or "NYY-O" in Description
# MAGIC                 or "NYCWY" in Description
# MAGIC                 or "NYCY" in Description
# MAGIC             )
# MAGIC             and ("BN,BK,GY" in Core_Marking_Groups)
# MAGIC         )
# MAGIC         or (
# MAGIC             (cores == "4")
# MAGIC             and (
# MAGIC                 "NYM-O" in Description
# MAGIC                 or "NYY-O" in Description
# MAGIC                 or "NYCWY" in Description
# MAGIC                 or "NYCY" in Description
# MAGIC             )
# MAGIC             and ("BU,BN,BK,GY" in Core_Marking_Groups)
# MAGIC         )
# MAGIC         or (
# MAGIC             (cores == "5")
# MAGIC             and (
# MAGIC                 "NYM-O" in Description
# MAGIC                 or "NYY-O" in Description
# MAGIC                 or "NYCWY" in Description
# MAGIC                 or "NYCY" in Description
# MAGIC             )
# MAGIC             and ("BU,BN,BK,GY,BK" in Core_Marking_Groups)
# MAGIC         )
# MAGIC     ):
# MAGIC
# MAGIC         return f"{Description} NEW"
# MAGIC
# MAGIC     elif (
# MAGIC         (
# MAGIC             (cores == "2")
# MAGIC             and (
# MAGIC                 "H07RN-F" in Description
# MAGIC                 or "H03VV-F" in Description
# MAGIC                 or "H05VV-F" in Description
# MAGIC             )
# MAGIC             and ("BN,BU" in Core_Marking_Groups)
# MAGIC         )
# MAGIC         or (
# MAGIC             (cores == "3")
# MAGIC             and (
# MAGIC                 "H07RN-F" in Description
# MAGIC                 or "H03VV-F" in Description
# MAGIC                 or "H05VV-F" in Description
# MAGIC             )
# MAGIC             and ("BK,BN,BU" in Core_Marking_Groups)
# MAGIC         )
# MAGIC         or (
# MAGIC             (cores == "4")
# MAGIC             and (
# MAGIC                 "H07RN-F" in Description
# MAGIC                 or "H03VV-F" in Description
# MAGIC                 or "H05VV-F" in Description
# MAGIC             )
# MAGIC             and ("BK,BU,BN,BK" in Core_Marking_Groups)
# MAGIC         )
# MAGIC         or (
# MAGIC             (cores in "5")
# MAGIC             and (
# MAGIC                 "H07RN-F" in Description
# MAGIC                 or "H03VV-F" in Description
# MAGIC                 or "H05VV-F" in Description
# MAGIC             )
# MAGIC             and ("BK,BU,BN,BK,BK" in Core_Marking_Groups)
# MAGIC         )
# MAGIC     ):
# MAGIC
# MAGIC         return f"{Description} OLD"
# MAGIC
# MAGIC     elif (
# MAGIC         (
# MAGIC             (cores == "2")
# MAGIC             and (
# MAGIC                 "H07RN-F" in Description
# MAGIC                 or "H03VV-F" in Description
# MAGIC                 or "H05VV-F" in Description
# MAGIC             )
# MAGIC             and ("BU,BN" in Core_Marking_Groups)
# MAGIC         )
# MAGIC         or (
# MAGIC             (cores in "3")
# MAGIC             and (
# MAGIC                 "H07RN-F" in Description
# MAGIC                 or "H03VV-F" in Description
# MAGIC                 or "H05VV-F" in Description
# MAGIC             )
# MAGIC             and ("BN,BK,GY" in Core_Marking_Groups)
# MAGIC         )
# MAGIC         or (
# MAGIC             (cores in "4")
# MAGIC             and (
# MAGIC                 "H07RN-F" in Description
# MAGIC                 or "H03VV-F" in Description
# MAGIC                 or "H05VV-F" in Description
# MAGIC             )
# MAGIC             and ("BU,BN,BK,GY" in Core_Marking_Groups)
# MAGIC         )
# MAGIC         or (
# MAGIC             (cores == "5")
# MAGIC             and (
# MAGIC                 "H07RN-F" in Description
# MAGIC                 or "H03VV-F" in Description
# MAGIC                 or "H05VV-F" in Description
# MAGIC             )
# MAGIC             and ("BU,BN,BK,GY,BK" in Core_Marking_Groups)
# MAGIC         )
# MAGIC     ):
# MAGIC
# MAGIC         return f"{Description} NEW"
# MAGIC
# MAGIC     else:
# MAGIC
# MAGIC         return f"{Description}"
# MAGIC
# MAGIC
# MAGIC spark.udf.register("toGetCableDescription", to_get_cable_description, StringType())

