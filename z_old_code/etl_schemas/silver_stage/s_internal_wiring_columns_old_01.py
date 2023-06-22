from nf_common_source.code.nf.types.column_types import ColumnTypes


class SInternalWiring(ColumnTypes):
    DATABASE_NAME = "database_name"
    TO_MARKING = "to_marking"
    OBJECT_IDENTIFIER = "object_identifier"
    FROM_PARENT_EQUIPMENT_NO = "from_parent_equipment_no"
    FROM_COMPARTMENT = "from_compartment"
    FROM_EQUIPMENT = "from_equipment"
    FROM_WIRE_LINK = "from_wire_link"
    FROM_MARKING = "from_marking"
    FROM_LEFT_RIGHT = "from_left_right"
    TO_PARENT_EQUIPMENT_NO = "to_parent_equipment_no"
    TO_COMPARTMENT = "to_compartment"
    TO_EQUIPMENT_NO = "to_equipment_no"
    TO_WIRE_LINK = "to_wire_link"
    TO_LEFT_RIGHT = "to_left_right"
    CLASS = "class"
    PARQUET_FILE_RELATIVE_PATH = "parquet_file_relative_path"