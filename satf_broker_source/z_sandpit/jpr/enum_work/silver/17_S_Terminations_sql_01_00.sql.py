from enum import Enum

class STerminations(Enum):
    database_name = "database_name"
    object_identifier = "object_identifier"
    cablenumber = "cablenumber"
    core_markings = "core_markings"
    parent_equipment_no = "parent_equipment_no"
    equipment_no = "equipment_no"
    marking = "marking"
    left_right = "left_right"
    _class = "class"  # Because class is a reserved keyword in Python
    parquet_file_relative_path = "parquet_file_relative_path"