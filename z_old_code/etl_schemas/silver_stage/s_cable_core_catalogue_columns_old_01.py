from nf_common_source.code.nf.types.column_types import ColumnTypes


class S_CableCoreCatalogueColumns(ColumnTypes):
    OBJECT_IDENTIFIER = "object_identifier"
    DATABASE_NAME = "database_name"
    CABLE_OBJECT_IDENTIFIER = "cable_object_identifier"
    GROUP_MARKING = "group_marking"
    CORE_MARKINGS = "core_markings"
    GROUP_MARKING_SEQUENCE = "group_marking_sequence"
    CORE_MARKINGS_CORE_TYPE = "core_markings_core_type"
    IS_VALID_CABLE_AND_CORE = "is_valid_cable_and_core"
    PARQUET_FILE_RELATIVE_PATH = "parquet_file_relative_path"


