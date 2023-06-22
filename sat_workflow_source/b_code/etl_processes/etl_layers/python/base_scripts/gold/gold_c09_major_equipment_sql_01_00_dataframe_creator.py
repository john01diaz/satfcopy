from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames
from sat_workflow_source.b_code.etl_schemas.gold_stage.major_equipments import Major_Equipments
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_major_equipments import S_Major_Equipments


# Function to filter the dataframes
def filter_by_database_name(
        s_major_equipments_dataframe,
        database_names_dataframe):
    return s_major_equipments_dataframe[
        s_major_equipments_dataframe[S_Major_Equipments.DATABASE_NAME.value].isin(
                database_names_dataframe[DatabaseNames.DATABASE_NAME.value]
                )
    ]

def create_dataframe_gold_c09_major_equipment_sql_01_00(
        input_tables: dict):
    s_major_equipments_dataframe = \
        input_tables['S_Major_Equipments']
    
    database_names_dataframe = \
        input_tables['database_names']
    
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
    
    major_equipments_dataframe.replace(
            {
                None:'null'
                },
            inplace=True)

    return major_equipments_dataframe
