from enum import Enum

from nf_common_source.code.nf.types.column_types import ColumnTypes


class S_CableCatalogueNumberMasterColumns(ColumnTypes):
    CATALOGUE_NO = "catalogueno"
    PARQUET_FILE_RELATIVE_PATH = "parquet_file_relative_path"
    DATABASE_NAME = "database_name"
    DESCRIPTION = "description"
    CABLE_OBJECT_IDENTIFIER = "cable_object_identifier"
