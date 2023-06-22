from enum import Enum


class Layer(
        Enum):
    CS_CFC_STRUCTURE_LAYER = 'cs_cfc_structure_layer'
    CS_LOOP_UNIT = 'cs_loop_unit'
    CS_PRODUCTION_UNIT = 'cs_production_unit'
    DATABASE_NAME = 'database_name'
    DYNAMIC_CLASS = 'dynamic_class'
    LAYER_CS_ASSEMBLY_SITE = 'layer_cs_assembly_site'
    LAYER_CS_LOOP = 'layer_cs_loop'
    LAYER_CS_LOOP_REF = 'layer_cs_loop_ref'
    LAYER_DM_DOCUMENT = 'layer_dm_document'
    MASTER_DYN_CLASS = 'master_dyn_class'
    MASTER_HREF = 'master_href'
    NAME = 'name'
    OBJECT_IDENTIFIER = 'object_identifier'
    PARQUET_FILE_RELATIVE_PATH = 'parquet_file_relative_path'
    PDM_INVISIBLE = 'pdm_invisible'
    PV_DESCRIPTION = 'pv_description'
    PV_SEGNR = 'pv_segnr'
    TEMPLATE_LOOP = 'template_loop'
