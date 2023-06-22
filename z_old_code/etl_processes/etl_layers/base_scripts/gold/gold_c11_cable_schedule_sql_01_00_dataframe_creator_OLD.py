from typing import List
import pandas


COLUMN_RENAMES = \
    {
        "Cable_Drum_Number": "Cable Drum Number",
        "Laying_Corner_Point": "Laying Corner Point",
        "External_document_of_item": "External document of item",
        "Installed_Length": "Installed Length",
        "Installation_Date": "Installation Date",
        "Estimated_Length": "Estimated Length",
        "Function_text_1": "Function text 1",
        "Level_of_Installation": "Level of Installation",
        "Shield_number": "Shield number",
        "Wire_number": "Wire number",
        "Cable_Set": "Cable Set",
        "Wire_type": "Wire type",
        "Conductor_type": "Conductor type",
        "Insulating_material": "Insulating material",
        "Inductance_per_km": "Inductance Per km",
        "Bending_radius": "Bending radius",
        "Capacitance_per_km": "Capacitance Per km",
        "Shield": "Shield",
        "Rated_voltage_Uo": "Rated_voltage Uo",
        "Rated_voltage_U": "Rated_voltage U",
        "Precious_metal_factor_2": "Precious metal factor 2",
        "Precious_metal_factor_1": "Precious metal factor 1",
        "Suppliers_article_no": "Suppliers article number",
        "Mass": "Mass",
        "Component_description_1": "Component description 1",
        "Selection_key": "Selection key",
        "Outside_diameter": "Outside diameter",
        "Subassembly_information": "Subassembly information",
        "Rated_temperature": "Rated temperature",
        "Quantity_unit": "Quantity unit",
        "Mounting_feature": "Mounting feature",
        "min_ambient_temperature": "minimum ambient temperature",
        "Measure_unit_qualifier": "Measure unit qualifier",
        "max_ambient_temperature": "maximum ambient temperature",
        "List_price": "List price",
        "EAN_number": "EAN number",
        "Body_length": "Body length"
    }

# Note: added by hand to filter the set of columns
COLUMNS_TO_KEEP = \
    [
        'CableNumber',
        'From_Location',
        'To_Location',
        'CatalogueNo',
        'Length',
        'ProjectStatus',
        'Remarks',
        'Gland_From',
        'Gland_To',
        'Adapter_From',
        'Adapter_To',
        'Range',
        'Cable Drum Number',
        'Laying Corner Point',
        'External document of item',
        'Installed Length',
        'Installation Date',
        'Estimated Length',
        'Function text 1',
        'Level of Installation',
        'Shield number',
        'Wire number',
        'Cable Set',
        'Wire type',
        'Conductor type',
        'Insulating material',
        'Inductance Per km',
        'Bending radius',
        'Capacitance Per km',
        'Shield',
        'Rated_voltage Uo',
        'Rated_voltage U',
        'Precious metal factor 2',
        'Precious metal factor 1',
        'Suppliers article number',
        'Mass',
        'Component description 1',
        'Selection key',
        'Outside diameter',
        'Subassembly information',
        'Rated temperature',
        'Quantity unit',
        'Mounting feature',
        'minimum ambient temperature',
        'Measure unit qualifier',
        'maximum ambient temperature',
        'List price',
        'EAN number',
        'Body length'
    ]


def create_dataframe_gold_c11_cable_schedule_sql_01_00_v0_02(
        cable_schedule_dataframe: pandas.DataFrame,
        database_names_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    cable_schedule_dataframe = \
        __get_unique_rows(
            cable_schedule_dataframe)

    database_names = \
        database_names_dataframe['Database_name'].tolist()

    cable_schedule_filtered_dataframe = \
        __get_rows_by_database_name(
            cable_schedule_dataframe,
            database_names)

    cable_schedule_final_dataframe = \
        __rename_columns(
            cable_schedule_filtered_dataframe,
            COLUMN_RENAMES)

    # Note: added by hand to filter the set of columns
    cable_schedule_final_dataframe = \
        cable_schedule_final_dataframe.filter(
            items=COLUMNS_TO_KEEP)

    return \
        cable_schedule_final_dataframe


def __get_unique_rows(
        dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    return \
        dataframe.drop_duplicates()


def __get_rows_by_database_name(
        dataframe: pandas.DataFrame,
        database_names: List[str]) \
        -> pandas.DataFrame:
    return \
        dataframe[dataframe['database_name'].isin(
            database_names)]


def __rename_columns(
        dataframe: pandas.DataFrame,
        column_renames: dict) -> pandas.DataFrame:
    return \
        dataframe.rename(
            columns=column_renames)
