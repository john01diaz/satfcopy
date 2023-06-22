from typing import List

import pandas as pd

COLUMN_RENAMES = {
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
	"Body_length": "Body length",
	}


def read_csv(
		file_name: str) -> pd.DataFrame:
	return pd.read_csv(
		file_name)


def get_unique_rows(
		df: pd.DataFrame) -> pd.DataFrame:
	return df.drop_duplicates()


def get_rows_by_database_name(
		df: pd.DataFrame,
		database_names: List[str]) -> pd.DataFrame:
	return df[df['database_name'].isin(
		database_names)]


def rename_columns(
		df: pd.DataFrame,
		column_renames: dict) -> pd.DataFrame:
	return df.rename(
		columns=column_renames)


def _07_Loop_Index(
		cable_schedule_file: str,
		database_names_file: str) -> pd.DataFrame:
	cable_schedule_df = read_csv(
		cable_schedule_file)
	database_names_df = read_csv(
		database_names_file)

	cable_schedule_df = get_unique_rows(
		cable_schedule_df)

	database_names = database_names_df['Database_name'].tolist()
	cable_schedule_filtered_df = get_rows_by_database_name(
		cable_schedule_df,
		database_names)

	cable_schedule_final_df = rename_columns(
		cable_schedule_filtered_df,
		COLUMN_RENAMES)

	return cable_schedule_final_df

# Usage example:
# result_df = _07_Loop_Index("cable_schedule.csv",
