from enum import Enum


class S_ItemFunction(
        Enum):
    ADD = 'add'
    CHANNELNUMBER = 'channelnumber'
    CLASS = 'class'
    DATABASE_NAME = 'database_name'
    DESCRIPTION = 'description'
    DEVICE_TYPE = 'device_type'
    DOCUMENT_NUMBER = 'document_number'
    DYNAMIC_CLASS = 'dynamic_class'
    FACILITY_DESIGNATION = 'facility_designation'
    FACILITY_DYNAMIC_CLASS = 'facility_dynamic_class'
    FACILITY_OBJECT_IDENTIFIER = 'facility_object_identifier'
    FUNCTION_OCC_OBJECT_IDENTIFIER = 'function_occ_object_identifier'
    IOTYPE = 'iotype'
    ITEM_DYNAMIC_CLASS = 'item_dynamic_class'
    ITEM_OBJECT_IDENTIFIER = 'item_object_identifier'
    ITEM_SLOT = 'item_slot'
    LOCATION_DESIGNATION = 'location_designation'
    LOCATION_DYNAMIC_CLASS = 'location_dynamic_class'
    LOCATION_OBJECT_IDENTIFIER = 'location_object_identifier'
    LOOP_ELEMENT_DYNAMIC_CLASS = 'loop_element_dynamic_class'
    LOOP_ELEMENT_OBJECT_IDENTIFIER = 'loop_element_object_identifier'
    LOOP_NUMBER = 'loop_number'
    MANUFACTURER = 'manufacturer'
    METADATA = 'metadata'
    MODELNO = 'modelno'
    OBJECT_IDENTIFIER = 'object_identifier'
    PRODUCT_DESIGNATION = 'product_designation'
    PRODUCT_KEY = 'product_key'
    PRODUCT_KEY_ORIGINAL = 'product_key_original'
    PROTOCOL = 'protocol'
    RACK = 'rack'
    RACK_DYNAMIC_CLASS = 'rack_dynamic_class'
    RACK_OBJECT_IDENTIFIER = 'rack_object_identifier'
    REMOVE = 'remove'
    ROWIDHIGHWATERMARK = 'rowidhighwatermark'
    SHOW_KEY = 'show_key'
    SYMBOL_NAME = 'symbol_name'
    TAG_NUMBER = 'tag_number'
    TXN = 'txn'
    TYPE = 'type'
