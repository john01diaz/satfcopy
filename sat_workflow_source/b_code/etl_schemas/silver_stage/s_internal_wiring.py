from enum import Enum


class S_Internal_Wiring(
        Enum):
    CLASS = 'class'
    DATABASE_NAME = 'database_name'
    FROM_COMPARTMENT = 'from_compartment'
    FROM_EQUIPMENT = 'from_equipment'
    FROM_LEFT_RIGHT = 'from_left_right'
    FROM_MARKING = 'from_marking'
    FROM_PARENT_EQUIPMENT_NO = 'from_parent_equipment_no'
    FROM_WIRE_LINK = 'from_wire_link'
    OBJECT_IDENTIFIER = 'object_identifier'
    TO_COMPARTMENT = 'to_compartment'
    TO_EQUIPMENT_NO = 'to_equipment_no'
    TO_LEFT_RIGHT = 'to_left_right'
    TO_MARKING = 'to_marking'
    TO_PARENT_EQUIPMENT_NO = 'to_parent_equipment_no'
    TO_WIRE_LINK = 'to_wire_link'
