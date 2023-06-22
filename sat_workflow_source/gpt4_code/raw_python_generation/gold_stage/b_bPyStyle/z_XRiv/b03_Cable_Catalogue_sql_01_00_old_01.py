from typing import (
	List)

import pandas as pd


def read_csv_to_dataframe(
		file_name: str) \
		-> pd.DataFrame:
	return pd.read_csv(
		file_name)


def create_immutable_dataframe(
		dataframe: pd.DataFrame) \
		-> pd.DataFrame:
	return dataframe.copy()


def filter_classes(
		dataframe: pd.DataFrame,
		classes: List[str]) \
		-> pd.DataFrame:
	return dataframe[dataframe['Class'].isin(
		classes)]


def filter_database_names(
		dataframe: pd.DataFrame,
		database_names: pd.DataFrame) \
		-> pd.DataFrame:
	return dataframe[dataframe['database_name'].isin(
		database_names['Database_name'])]


def merge_dataframes(
		cable_catalogue: pd.DataFrame,
		cable_master: pd.DataFrame) \
		-> pd.DataFrame:
	return cable_catalogue.merge(
		cable_master,
		how='inner',
		left_on=['database_name', 'Object_Identifier'],
		right_on=['database_name', 'Cable_Object_Identifier'])


def calculate_row_number(
		dataframe: pd.DataFrame) \
		-> pd.DataFrame:
	dataframe['RNT'] = dataframe.groupby(
		'CatalogueNo').cumcount() + 1
	return dataframe


def filter_by_row_number(
		dataframe: pd.DataFrame,
		row_number: int) \
		-> pd.DataFrame:
	return dataframe[dataframe['RNT'] == row_number]


def cable_core_catalogue(
		cable_catalogue_file: str,
		cable_master_file: str,
		database_names_file: str) \
		-> pd.DataFrame:
	cable_catalogue = create_immutable_dataframe(
		read_csv_to_dataframe(
			cable_catalogue_file))
	cable_master = create_immutable_dataframe(
		read_csv_to_dataframe(
			cable_master_file))
	database_names = create_immutable_dataframe(
		read_csv_to_dataframe(
			database_names_file))

	filtered_classes = filter_classes(
		cable_catalogue,
		['Instrumentation', 'Inst(Shared)', 'Elec(Shared)'])
	filtered_database_names = filter_database_names(
		filtered_classes,
		database_names)
	merged_data = merge_dataframes(
		filtered_database_names,
		cable_master)
	row_number_data = calculate_row_number(
		merged_data)
	result = filter_by_row_number(
		row_number_data,
		1)

	return result


result = cable_core_catalogue(
	'cable_catalogue.csv',
	'cable_master.csv',
	'database_names.csv')
print(
	result)
