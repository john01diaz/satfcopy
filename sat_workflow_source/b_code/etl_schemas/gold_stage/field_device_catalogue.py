from enum import Enum


class Field_Device_Catalogue(
        Enum):
    CATALOGUE_NAME = 'catalogue_name'
    LEFT_PIN_DETAILS = 'left_pin_details'
    LEFT_MARKING = 'left_marking'
    RIGHT_PIN_DETAILS = 'right_pin_details'
    RIGHT_MARKING = 'right_marking'
    TAG_NUMBER = 'tag_number'
    LOOP_NUMBER = 'loop_number'
    DOCUMENT_NUMBER = 'document_number'
