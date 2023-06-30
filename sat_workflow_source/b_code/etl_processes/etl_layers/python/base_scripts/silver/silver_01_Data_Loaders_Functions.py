# Databricks notebook source
# %run /Users/p.abhishek@shell.com/01_Instrumentation_ETL_Scripts/03_Silver_Layer/00_Reusable_Functions

# COMMAND ----------

import json
# import re

# from pyspark.shell import spark
from pyspark.sql.functions import udf, regexp_replace
# from pyspark.sql.types import StringType, ArrayType

# from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.Enumerations_get_enumeration_display_value import \
#     get_enumeration_display_value


# COMMAND ----------

def format_tag(loop_element_id):
    if loop_element_id:
        loop_element_id = loop_element_id.strip()
    else:
        return None
    if re.search("(^[A-Z]+)", loop_element_id):
        return "RHLND Instrument Tag 9"
    elif loop_element_id.isalnum():
        return "RHLND Instrument Tag 2"
    elif "/" in loop_element_id:
        return "RHLND Instrument Tag 4"
    elif " " in loop_element_id:
        return "RHLND Instrument Tag 5"
    elif "." in loop_element_id and any(
        i not in loop_element_id
        for i in ["!", "@", "$", "^", "&", "-", ";", ":", "?", "#", "*", "/"]
    ):
        dot_index = loop_element_id.index(".")
        underscore_index = loop_element_id.index("_") if "_" in loop_element_id else 0
        if re.search("(^[0-9]+)([A-Z]+)([0-9]+)([A-Z])", loop_element_id):
            if (
                re.search("(^[0-9]+)([A-Z]+)([0-9]+)([A-Z])", loop_element_id)
                .groups()[3]
                .isalpha()
            ):
                return "RHLND Instrument Tag 2"
        if dot_index < underscore_index or underscore_index == 0:
            return "RHLND Instrument Tag 1"
        else:
            return "RHLND Instrument Tag 3"
    elif "-" in loop_element_id:
        if re.search("(^[0-9]+)([A-Z]+)([\-])", loop_element_id):
            if re.search("([0-9]+)([A-Z]+)([\-])", loop_element_id).groups()[2] == "-":
                return "RHLND Instrument Tag 8"
        elif re.search("(^[0-9]+)([A-Z]+)([0-9]+)([A-Z]+)", loop_element_id):
            if (
                re.search("(^[0-9]+)([A-Z]+)([0-9]+)([A-Z])", loop_element_id)
                .groups()[3]
                .isalpha()
            ):
                return "RHLND Instrument Tag 2"
        elif re.search("(^[0-9]+)([A-Z]+)([0-9]+)([\-])", loop_element_id):
            if (
                re.search("(^[0-9]+)([A-Z]+)([0-9]+)([\-])", loop_element_id).groups()[
                    3
                ]
                == "-"
            ):
                return "RHLND Instrument Tag 6"
    elif "_" in loop_element_id:
        return "RHLND Instrument Tag 3"
    else:
        return "RHLND Instrument Tag 2"

# COMMAND ----------

# %python
# MAGIC
# MAGIC
# @udf(returnType=StringType())
def get_process_unit(layer_Pv_base_element, layer_CS_loop_unit):
    if layer_Pv_base_element == None:
        return None
    list = ["[", "(", ")", "]"]
    if any(layer_CS_loop_unit for i in list):
        if re.findall("\[.*?\]", layer_CS_loop_unit):
            element = re.findall("\[.*?\]", layer_CS_loop_unit)[0]
            layer_CS_loop_unit = element[1 : len(element) - 1]
# MAGIC
    if any(i in layer_Pv_base_element for i in list):
        base_element = layer_Pv_base_element
        base_element = re.sub(r"[\([)\]]", "", base_element).strip()
        base_element = re.sub(r" ", "_", base_element)
        base_element = re.sub(r"-", "", base_element)
    else:
        if layer_CS_loop_unit is None:
            base_element = layer_Pv_base_element.strip()
            base_element = re.sub(r" ", "_", base_element)
            base_element = re.sub(r"-", "", base_element)
        else:
            base_element = layer_CS_loop_unit + "_" + layer_Pv_base_element
            base_element = re.sub(r" ", "_", base_element)
            base_element = re.sub(r"-", "", base_element)
    base_element = (
        base_element[0 : len(base_element) - 1]
        if base_element.endswith("_")
        else base_element
    )
    return base_element

# COMMAND ----------

def loop_element_id_suffix(loop_element_id, area, function, sequence_num):
    if loop_element_id is None:
        return None
    area = "" if area is None else area
    function = "" if function is None else function
    sequence_num = "" if sequence_num is None else sequence_num
    id = area + function + sequence_num
    len_id = len(id)
    suffix_index = loop_element_id.find(id)
    suffix_starts = suffix_index + len_id
    if suffix_index != -1:
        suffix = loop_element_id[suffix_starts:]
        suffix = (
            suffix[1:]
            if suffix.startswith(".")
            or suffix.startswith("/")
            or suffix.startswith("_")
            or suffix.startswith("-")
            or suffix.startswith("*")
            or suffix.startswith(" ")
            else suffix
        )
        suffix = (
            suffix[1:]
            if suffix.startswith(".")
            or suffix.startswith("/")
            or suffix.startswith("_")
            or suffix.startswith("-")
            else suffix
        )
        suffix = suffix[0 : len(suffix) - 1] if suffix.endswith(".") else suffix
        return suffix
    else:
        return "Not_found"

# COMMAND ----------

# %python
# spark.udf.register("formatTag", format_tag, StringType())
# spark.udf.register("loopelementSuffix", loop_element_id_suffix, StringType())
# spark.udf.register("get_process_unit", get_process_unit)
# spark.udf.register("get_enumeration_display_value", get_enumeration_display_value)

# COMMAND ----------

# @udf(returnType=StringType())
def get_process_unit(layer_Pv_base_element, layer_CS_loop_unit):
    if layer_Pv_base_element == None:
        return None
    list = ["[", "(", ")", "]"]
    if any(layer_CS_loop_unit for i in list):
        if re.findall("\[.*?\]", layer_CS_loop_unit):
            element = re.findall("\[.*?\]", layer_CS_loop_unit)[0]
            layer_CS_loop_unit = element[1 : len(element) - 1]

    if any(i in layer_Pv_base_element for i in list):
        base_element = layer_Pv_base_element
        base_element = re.sub(r"[\([)\]]", "", base_element).strip()
        base_element = re.sub(r" ", "_", base_element)
        base_element = re.sub(r"-", "", base_element)
    else:
        if layer_CS_loop_unit is None:
            base_element = layer_Pv_base_element.strip()
            base_element = re.sub(r" ", "_", base_element)
            base_element = re.sub(r"-", "", base_element)
        else:
            base_element = layer_CS_loop_unit + "_" + layer_Pv_base_element
            base_element = re.sub(r" ", "_", base_element)
            base_element = re.sub(r"-", "", base_element)
    base_element = (
        base_element[0 : len(base_element) - 1]
        if base_element.endswith("_")
        else base_element
    )
    return base_element


# @udf(returnType=StringType())
def lno_column(loopid, looppv):
    if looppv is None:
        return None

    sequence_no = loopid.split(looppv)[1]
    if sequence_no.isdigit():
        return sequence_no
    else:
        sequence_no = re.findall(r"[\d]*", sequence_no)[0]
        return sequence_no


# @udf(returnType=StringType())
def loopsuffix_column(loopid, sequ):
    if sequ is None or len(sequ) == 0:
        return None
    else:
        suffix = loopid.split(sequ)[1]
        suffix = (
            suffix[1:]
            if suffix.startswith("_")
            or suffix.startswith(".")
            or suffix.startswith("/")
            else suffix
        )
        return suffix


# @udf(returnType=StringType())
def loopsuffix_column_2(loop_id, LoopPV):
    if LoopPV is None or len(LoopPV) == 0:
        return None
    index = loop_id.find(LoopPV) + len(LoopPV)
    suffix = loop_id[index:]
    suffix = (
        suffix[1:]
        if suffix.startswith("_") or suffix.startswith(".") or suffix.startswith("/")
        else suffix
    )
    return suffix


# spark.udf.register("get_process_unit", get_process_unit)
# spark.udf.register("get_enumeration_display_value", get_enumeration_display_value)

# COMMAND ----------

# %python
# MAGIC
# MAGIC
# @udf(returnType=ArrayType(StringType()))
def array_elements_sorting(column_list):
    column_list_1 = [i for i in column_list if i != ""]
    with_pin_group = []
    without_pin_group = []
    with_alpha_pins = []
    for i in column_list_1:
        if ":" in i:
            with_pin_group.append(i)
        elif i.isalpha():
            with_alpha_pins.append(i)
        else:
            without_pin_group.append(i)
# MAGIC
    without_pin_group.sort(
        key=lambda x: int(re.search("(\d+)", x).group(1))
        if (re.search("(\d+)[A-Za-z+-]*", x))
        else 99
    )
    final_list = without_pin_group + sorted(with_alpha_pins) + sorted(with_pin_group)
    return final_list
# MAGIC
# MAGIC
# spark.udf.register("array_elements_sorting", array_elements_sorting)

# COMMAND ----------

# %sql
# Create
# or replace temp view VW_Database_names As
# Select
#   explode(Split('R_2016R3', ',')) as Database_name

# COMMAND ----------

# %python
# MAGIC
# MAGIC
# @udf(returnType=StringType())
def get_terminal_marking(col, req):
    if col is None:
        return ""
    # req = 0 then return count of terminals
    if req == 0:
        col_list = col.split(" ", 1)
        return col_list[0]
    # req != 0 then return terminal markings
    else:
        col_list = col.split(" ", 1)
        if col_list[0] == "0":
            return None
        terminals = col_list[1][1 : len(col_list[1]) - 1]
        terminals_list = terminals.split(",")
        new_terminal_list = []
        count = 1
        for terminal in terminals_list:
            terminal = terminal.strip()
            if terminal != "":
                new_terminal_list.append(terminal)
            else:
                element = "NA" + str(count)
                new_terminal_list.append(element)
                count += 1
        if len(new_terminal_list) < int(col_list[0]):
            n = int(col_list[0]) - len(new_terminal_list)
            for i in range(n):
                element = "NA" + str(count)
                new_terminal_list.append(element)
                count += 1
        int_terminal_list = sorted(
            [int(text) for text in new_terminal_list if text.isdigit()]
        )
        string_terminal_list = sorted(
            [text for text in new_terminal_list if not text.isdigit()]
        )
        terminal_list = [str(i) for i in int_terminal_list] + string_terminal_list
        return ",".join(terminal_list)
# MAGIC
# MAGIC
# spark.udf.register("get_terminal_marking", get_terminal_marking)

# COMMAND ----------

# %python
# MAGIC
import re
# MAGIC
# MAGIC
def sorted_nicely(l):
    convert = lambda text: int(text) if text.isdigit() else text
    alphanum_key = lambda key: [convert(c) for c in re.split("([0-9]+)", key)]
    return sorted(l, key=alphanum_key)
# MAGIC
# MAGIC
# @udf(returnType=StringType())
def to_get_sort(col):
    col = sorted_nicely([i for i in col if i != ""])
    return ",".join(col)
# MAGIC
# MAGIC
# spark.udf.register("to_get_sort", to_get_sort)

# COMMAND ----------

# @udf(returnType=StringType())
def to_get_loop_format_tag(loop_id):
    loop_id = loop_id.strip()
    if re.search("^[0-9]+[A-Z]+[0-9]+[_][0-9A-Z]+", loop_id):
        return "RHLND Loop Tag 2"
    if re.search("^[0-9]+[A-Z]+[0-9]+[ ][0-9A-Z]+", loop_id):
        return "RHLND Loop Tag 2"
    elif re.search("^[0-9]+[A-Z]+[0-9]+[.][0-9A-Z]+", loop_id):
        return "RHLND Loop Tag 3"
    elif re.search("^[0-9]+[A-Z]+[0-9]+[/][0-9A-Z]+", loop_id):
        return "RHLND Loop Tag 4"
    elif re.search("^[A-Z]+[0-9]+[_][0-9A-Z]+", loop_id):
        return "RHLND Loop Tag 5"
    elif re.search("^[A-Z]+[_][0-9A-Z]+", loop_id):
        return "RHLND Loop Tag 7"
    elif re.search("^[A-Z]+[0-9]+[A-Z0-9]+", loop_id):
        return "RHLND Loop Tag 9"
    elif re.search("^[0-9]+[A-Z]+[0-9]+[-][0-9A-Z]+", loop_id):
        return "RHLND Loop Tag 6"
    elif re.search("^[A-Z]+[_][A-Z0-9]+[.][A-Z0-9]+", loop_id):
        return "RHLND Loop Tag 11"
    elif re.search("^[0-9A-Z]+$", loop_id):
        return "RHLND Loop Tag 1"
    else:
        return "NO TAG"


# spark.udf.register("to_get_loop_format_tag", to_get_loop_format_tag)

# COMMAND ----------

# %python
colour_modification_enum = {
    "BLACK": "BK",
    "SCHWARZ": "BK",
    "SW": "BK",
    "BRAUN": "BN",
    "BROWN": "BN",
    "BR": "BN",
    "BLAU": "BU",
    "BLUE": "BU",
    "BL": "BU",
    "GRÜN": "GN",
    "GREEN": "GN",
    "GRU": "GN",
    "GN": "GN",
    "GRAU": "GY",
    "GREY": "GY",
    "GR": "GY",
    "ORANGE": "OG",
    "ORG": "OG",
    "OR": "OG",
    "ROSA": "PK",
    "PINK": "PK",
    "RO": "PK",
    "RS": "PK",
    "ROT": "RD",
    "RED": "RD",
    "RT": "RD",
    "TK": "TQ",
    "VIO": "VT",
    "VIOL": "VT",
    "VIOLET": "VT",
    "VIOLETT": "VT",
    "VI": "VT",
    "WEIß": "WH",
    "WHITE": "WH",
    "WS": "WH",
    "GELB": "YE",
    "YELLOW": "YE",
    "GE": "YE",
}
# MAGIC
# MAGIC
def get_colour_revised(c):
    if c:
        c = c.upper().replace("|", "")
    else:
        return None
    if any(c == color for color in colour_modification_enum.keys()):
        c = c.replace(c, colour_modification_enum[c])
        return c
    else:
        for color in colour_modification_enum.keys():
            if color in c:
                c = c.replace(color, colour_modification_enum[color])
        return c
# MAGIC
# MAGIC
# spark.udf.register("getColourRevised", get_colour_revised, StringType())

# COMMAND ----------

# %python
# MAGIC
# MAGIC
def getCableCatalogue(c, t):
    t = t.upper()
    if c:
        c = c.upper()
    else:
        return None
    if t == "OAS_Coll_SH":
        if any(i in c for i in ["ST", "K", "C"]):
            return "TRUE"
    elif t == "GSCR":
        GScr = ["PIMF", "TIMF"]
        if any(i in c for i in GScr):
            return "TRUE"
    elif t == "ARMOURED":
        if "RE%" in c and any(i in c for i in ["SWA", "RG", "FG", "B", "Q"]):
            return "TRUE"
        elif "N%" in c and any(i in c for i in ["B", "G", "F", "R"]):
            return "TRUE"
        elif "A%" in c and "B" in c:
            return "TRUE"
        elif "J%" in c and "B" in c:
            return "TRUE"
        elif "S%" in c and "B" in c:
            return "TRUE"
    elif t == "ARMOURDESCRIPTION":
        if "N%" in c and "B" in c:
            return "Steel tape armouring"
        elif "N%" in c and "F" in c:
            return "Armouring of galvanized flat steel wire"
        elif "N%" in c and "G" in c:
            return "Helix of galvanized steel tape"
        elif "N%" in c and "R" in c:
            return "Armouring of galvanized round steel wires"
        elif "RE%" in c and "SWA" in c:
            return "Galvanised round steel wires"
        elif "RE%" in c and "RG" in c:
            return "Galvanised round steel wires with counter helix made of galvanised steel tape"
        elif "RE%" in c and "FG" in c:
            return "Galvanised flat steel wires with counter helix made of galvanised steel tape"
        elif "RE%" in c and "B" in c:
            return "Double layer made of galvanised steel tape"
        elif "RE%" in c and "Q" in c:
            return "Braide made of galvanised steel tape"
        elif (
            ("A%" in c and "B" in c)
            or ("J%" in c and "B" in c)
            or ("S%" in c and "B" in c)
        ):
            return "Armouring"
    else:
        return "FALSE"
# MAGIC
# MAGIC
# spark.udf.register("getCableCatalogue", getCableCatalogue)

# COMMAND ----------

def multiple_replace(column, str):
    dict = json.loads(str)
    if column:
        for old_string in dict.keys():
            column = column.upper().replace(old_string, dict[old_string])
    return column


# spark.udf.register("multipleReplace", multiple_replace, StringType())

# COMMAND ----------

# %python
# MAGIC
# MAGIC
def to_get_cable_description(Description, Core_Marking_Groups, cores, EarthCore):
    if EarthCore.upper() == "TRUE":
        if (
            cores == "3"
            and "NYM-J" in Description
            or "NYY-J" in Description
            and "BK,BU" in Core_Marking_Groups
        ) or (
            cores == "3"
            and "H07RN-F" in Description
            or "H03VV-F" in Description
            or "H05VV-F" in Description
            and "BN,BU" in Core_Marking_Groups
        ):
# MAGIC
            return f"{Description} OLD"
# MAGIC
        elif (
            cores == "3"
            and "NYM-J" in Description
            or "NYY-J" in Description
            and "BU,BN" in Core_Marking_Groups
        ) or (
            cores == "3"
            and "H07RN-F" in Description
            or "H03VV-F" in Description
            or "H05VV-F" in Description
            and "BU,BN" in Core_Marking_Groups
        ):
# MAGIC
            return f"{Description} NEW"
# MAGIC
        elif (
            cores == "4"
            and "NYM-J" in Description
            or "NYY-J" in Description
            and "BK,BU,BN" in Core_Marking_Groups
        ) or (
            cores == "4"
            and "H07RN-F" in Description
            or "H03VV-F" in Description
            or "H05VV-F" in Description
            and "BK,BU,BN" in Core_Marking_Groups
        ):
# MAGIC
            return f"{Description} OLD"
# MAGIC
        elif (
            cores == "4"
            and "NYM-J" in Description
            or "NYY-J" in Description
            and "BN,BK,GY" in Core_Marking_Groups
        ) or (
            cores == "4"
            and "H07RN-F" in Description
            or "H03VV-F" in Description
            or "H05VV-F" in Description
            and "BN,BK,GY" in Core_Marking_Groups
        ):
# MAGIC
            return f"{Description} NEW"
# MAGIC
        elif (
            cores == "5"
            and "NYM-J" in Description
            or "NYY-J" in Description
            and "BK,BU,BN,BK" in Core_Marking_Groups
        ) or (
            cores == "5"
            and "H07RN-F" in Description
            or "H03VV-F" in Description
            or "H05VV-F" in Description
            and "BK,BU,BN,BK" in Core_Marking_Groups
        ):
# MAGIC
            return f"{Description} OLD"
# MAGIC
        elif (
            cores == "5"
            and "NYM-J" in Description
            or "NYY-J" in Description
            and "BU,BN,BK,GY" in Core_Marking_Groups
        ) or (
            cores == "5"
            and "H07RN-F" in Description
            or "H03VV-F" in Description
            or "H05VV-F" in Description
            and "BU,BN,BK,GY" in Core_Marking_Groups
        ):
# MAGIC
            return f"{Description} NEW"
# MAGIC
        else:
# MAGIC
            return f"{Description}"
# MAGIC
    elif (
        (
            (cores == "2")
            and (
                "NYM-O" in Description
                or "NYY-O" in Description
                or "NYCWY" in Description
                or "NYCY" in Description
            )
            and ("BK,BU" in Core_Marking_Groups)
        )
        or (
            (cores == "3")
            and (
                "NYM-O" in Description
                or "NYY-O" in Description
                or "NYCWY" in Description
                or "NYCY" in Description
            )
            and ("BK,BU,BN" in Core_Marking_Groups)
        )
        or (
            (cores == "4")
            and (
                "NYM-O" in Description
                or "NYY-O" in Description
                or "NYCWY" in Description
                or "NYCY" in Description
            )
            and ("BK,BU,BN,BK" in Core_Marking_Groups)
        )
        or (
            (cores == "5")
            and (
                "NYM-O" in Description
                or "NYY-O" in Description
                or "NYCWY" in Description
                or "NYCY" in Description
            )
            and ("BK,BU,BN,BK,BK" in Core_Marking_Groups)
        )
    ):
# MAGIC
        return f"{Description} OLD"
# MAGIC
    elif (
        (
            (cores == "2")
            and (
                "NYM-O" in Description
                or "NYY-O" in Description
                or "NYCWY" in Description
                or "NYCY" in Description
            )
            and ("BU,BN" in Core_Marking_Groups)
        )
        or (
            (cores == "3")
            and (
                "NYM-O" in Description
                or "NYY-O" in Description
                or "NYCWY" in Description
                or "NYCY" in Description
            )
            and ("BN,BK,GY" in Core_Marking_Groups)
        )
        or (
            (cores == "4")
            and (
                "NYM-O" in Description
                or "NYY-O" in Description
                or "NYCWY" in Description
                or "NYCY" in Description
            )
            and ("BU,BN,BK,GY" in Core_Marking_Groups)
        )
        or (
            (cores == "5")
            and (
                "NYM-O" in Description
                or "NYY-O" in Description
                or "NYCWY" in Description
                or "NYCY" in Description
            )
            and ("BU,BN,BK,GY,BK" in Core_Marking_Groups)
        )
    ):
# MAGIC
        return f"{Description} NEW"
# MAGIC
    elif (
        (
            (cores == "2")
            and (
                "H07RN-F" in Description
                or "H03VV-F" in Description
                or "H05VV-F" in Description
            )
            and ("BN,BU" in Core_Marking_Groups)
        )
        or (
            (cores == "3")
            and (
                "H07RN-F" in Description
                or "H03VV-F" in Description
                or "H05VV-F" in Description
            )
            and ("BK,BN,BU" in Core_Marking_Groups)
        )
        or (
            (cores == "4")
            and (
                "H07RN-F" in Description
                or "H03VV-F" in Description
                or "H05VV-F" in Description
            )
            and ("BK,BU,BN,BK" in Core_Marking_Groups)
        )
        or (
            (cores in "5")
            and (
                "H07RN-F" in Description
                or "H03VV-F" in Description
                or "H05VV-F" in Description
            )
            and ("BK,BU,BN,BK,BK" in Core_Marking_Groups)
        )
    ):
# MAGIC
        return f"{Description} OLD"
# MAGIC
    elif (
        (
            (cores == "2")
            and (
                "H07RN-F" in Description
                or "H03VV-F" in Description
                or "H05VV-F" in Description
            )
            and ("BU,BN" in Core_Marking_Groups)
        )
        or (
            (cores in "3")
            and (
                "H07RN-F" in Description
                or "H03VV-F" in Description
                or "H05VV-F" in Description
            )
            and ("BN,BK,GY" in Core_Marking_Groups)
        )
        or (
            (cores in "4")
            and (
                "H07RN-F" in Description
                or "H03VV-F" in Description
                or "H05VV-F" in Description
            )
            and ("BU,BN,BK,GY" in Core_Marking_Groups)
        )
        or (
            (cores == "5")
            and (
                "H07RN-F" in Description
                or "H03VV-F" in Description
                or "H05VV-F" in Description
            )
            and ("BU,BN,BK,GY,BK" in Core_Marking_Groups)
        )
    ):
# MAGIC
        return f"{Description} NEW"
# MAGIC
    else:
# MAGIC
        return f"{Description}"
# MAGIC
# MAGIC
# spark.udf.register("toGetCableDescription", to_get_cable_description, StringType())

# COMMAND ----------

def custom_sort_pairs(items):
    pair_cables_list = []
    pair1,pair2,pair3,pair4,others = [],[],[],[],[] 
    priority_values = {
        'BU' : 1,
        'RD' : 1,
        'OG' : 1,
        'GY' : 2,
        'YE' : 2,
        'GN' : 3,
        'BN' : 3,
        'WH' : 4,
        'BK' : 4
    }
    for item in items:
        if item in priority_values.keys():
            if priority_values[item] == 1:
                pair1.append(item)
            elif priority_values[item] == 2:
                pair2.append(item)
            elif priority_values[item] == 3:
                pair3.append(item)
            elif priority_values[item] == 4:
                pair4.append(item)
        else:
            others.append(item)
    pair_cables_list.append(pair1)
    pair_cables_list.append(pair2)
    pair_cables_list.append(pair3)
    pair_cables_list.append(pair4)
    pair_cables_list.append(others)

    return pair_cables_list


def find_group_type(cable_colours):
    cable_colours = cable_colours.strip()
    if len(cable_colours) == 0:
        return "Cores" 
    colours = [item for item in cable_colours.split(",") if len(item.strip()) > 0]
    if 'SC' in colours:
        colours.remove('SC')
    if 'SL' in colours:
        colours.remove('SL')
    no_of_colours = len(colours)

    if no_of_colours%2 != 0 or no_of_colours == 0:
        return "Cores"

    else:
        sorted_pairs  = custom_sort_pairs(colours)
        if len(sorted_pairs[4]) >0 :
            return "Cores"
        elif all(len(pair) ==2 for pair in sorted_pairs[0:4] if len(pair)!=0):
            return "Pairs"
        else:
            return "Cores"

# spark.udf.register("FindGroupType", find_group_type, StringType())

# COMMAND ----------

def io_terminal_marking(num):
    random_number_dict = {}
    k = 1
    for i in range(65,91):
        for j in range(65,91,2):
            random_number_dict[k] = chr(i)+chr(j)+" "+"EMPTY" +","+chr(i)+chr(j+1)+" "+"EMPTY"
            k+=1
        if num < k:
            break
    return random_number_dict[num]
         

# spark.udf.register("ioTerminalMarking", io_terminal_marking, StringType())

# COMMAND ----------

# %python
def cleansing_df(df):
    df_columns = df.columns
    for column in df_columns:
        df = df.withColumn(column,regexp_replace(column,"\n",""))
    return df

# COMMAND ----------

def io_model_terminal_marking(model_no,channel_no):
	dict_10101 = {
		"1" : "36",
		"2" : "33",
		"3" : "32",
		"4" : "29",
		"5" : "28",
		"6" : "25",
		"7" : "24",
		"8" : "21",
		"9" : "20",
		"10": "17",
		"11": "16",
		"12": "13",
		"13": "12",
		"14": "9",
		"15": "8",
		"16": "5"
	}
	if "10101/2/1" in model_no:
		if channel_no in dict_10101.keys():
			return dict_10101[channel_no]
		else:
			return None
	else:
		return None

# spark.udf.register("ioModelTerminalMarking", io_model_terminal_marking, StringType())

# COMMAND ----------

def column_renamed(df_rename,value):
    for column in df_rename.columns:
        df_rename = df_rename.withColumnRenamed(column,f"{value}_{column}")
    return df_rename