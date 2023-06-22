from enum import Enum
import pandas


# Define the enums for each table
class DatabaseNames(
        Enum):
    DATABASE_NAME = 'database_name'


class Major_Equipments(
        Enum):
    PARENT_EQUIPMENT_NO = 'parent_equipment_no'
    EQUIPMENTTYPE = 'equipmenttype'
    AREA = 'area'
    DESCRIPTION = 'description'
    VENDORSUPPLIED = 'vendorsupplied'
    DWGREQUIRED = 'dwgrequired'
    STATUS = 'status'
    AREAPATH = 'areapath'
    TYPE = 'type'
    DESIGNATION = 'designation'
    COMMENT = 'comment'
    INSTALLATION_SITE = 'installation_site'
    CATEGORY = 'category'


class S_Major_Equipments(
        Enum):
    AREA = 'area'
    AREAPATH = 'areapath'
    CATEGORY = 'category'
    COMMENT = 'comment'
    DATABASE_NAME = 'database_name'
    DESCRIPTION = 'description'
    DESIGNATION = 'designation'
    DWGREQUIRED = 'dwgrequired'
    DYNAMIC_CLASS = 'dynamic_class'
    EQUIPMENTTYPE = 'equipmenttype'
    INSTALLATION_SITE = 'installation_site'
    OBJECT_IDENTIFIER = 'object_identifier'
    PARENT_EQUIPMENT_NO = 'parent_equipment_no'
    PARQUET_FILE_RELATIVE_PATH = 'parquet_file_relative_path'
    STATUS = 'status'
    TYPE = 'type'
    VENDORSUPPLIED = 'vendorsupplied'


# Function to filter the dataframes
def filter_by_database_name(
        s_major_equipments_dataframe,
        database_names_dataframe):
    return s_major_equipments_dataframe[
        s_major_equipments_dataframe[S_Major_Equipments.DATABASE_NAME.value].isin(
                database_names_dataframe[DatabaseNames.DATABASE_NAME.value]
                )
    ]


# Main transformation function
def transform_major_equipments(
        s_major_equipments_dataframe,
        database_names_dataframe):
    filtered_s_major_equipments_dataframe = filter_by_database_name(
        s_major_equipments_dataframe,
        database_names_dataframe)
    
    major_equipments_dataframe = filtered_s_major_equipments_dataframe[
        [
            S_Major_Equipments.AREA.value,
            S_Major_Equipments.PARENT_EQUIPMENT_NO.value,
            S_Major_Equipments.DESCRIPTION.value,
            S_Major_Equipments.EQUIPMENTTYPE.value,
            S_Major_Equipments.VENDORSUPPLIED.value,
            S_Major_Equipments.DWGREQUIRED.value,
            S_Major_Equipments.STATUS.value,
            S_Major_Equipments.AREAPATH.value,
            S_Major_Equipments.TYPE.value,
            S_Major_Equipments.DESIGNATION.value,
            S_Major_Equipments.COMMENT.value,
            S_Major_Equipments.INSTALLATION_SITE.value,
            S_Major_Equipments.CATEGORY.value,
            ]
    ]
    major_equipments_dataframe = major_equipments_dataframe.drop_duplicates()
    major_equipments_dataframe.rename(
        columns={
            S_Major_Equipments.INSTALLATION_SITE.value: Major_Equipments.INSTALLATION_SITE.value
            },
        inplace=True)
    
    return major_equipments_dataframe