from typing import (
	List)

import pandas


# def read_csv_to_dataframe(
# 		file_name: str) \
# 		-> pandas.DataFrame:
# 	return pandas.read_csv(
# 		file_name)


def create_immutable_dataframe(
		dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	return dataframe.copy()


def filter_classes(
		dataframe: pandas.DataFrame,
		classes: List[str]) \
		-> pandas.DataFrame:
	return dataframe[dataframe['Class'].isin(
		classes)]


def filter_database_names(
		dataframe: pandas.DataFrame,
		database_names: pandas.DataFrame) \
		-> pandas.DataFrame:
	return dataframe[dataframe['database_name'].isin(
		database_names['Database_name'])]


def merge_dataframes(
		cable_catalogue: pandas.DataFrame,
		cable_master: pandas.DataFrame) \
		-> pandas.DataFrame:
	return cable_catalogue.merge(
		cable_master,
		how='inner',
		left_on=['database_name', 'Object_Identifier'],
		right_on=['database_name', 'Cable_Object_Identifier'])


def calculate_row_number(
		dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	dataframe['RNT'] = dataframe.groupby(
		'CatalogueNo').cumcount() + 1
	return dataframe


def filter_by_row_number(
		dataframe: pandas.DataFrame,
		row_number: int) \
		-> pandas.DataFrame:
	return \
		dataframe[dataframe['RNT'] == row_number]


def create_cable_catalogue(
		cable_catalogue_dataframe: pandas.DataFrame,
		cable_master_dataframe: pandas.DataFrame,
		database_names_dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	# cable_catalogue = create_immutable_dataframe(
	# 	read_csv_to_dataframe(
	# 		cable_catalogue_file))
	# cable_master = create_immutable_dataframe(
	# 	read_csv_to_dataframe(
	# 		cable_master_file))
	# database_names = create_immutable_dataframe(
	# 	read_csv_to_dataframe(
	# 		database_names_file))

	# identical to list in c03_Cable_Core_Catalogue
	filter_list = \
		[
			'Instrumentation',
			'Inst(Shared)',
			'Elec(Shared)'
		]

	filtered_classes = \
		filter_classes(
			cable_catalogue_dataframe,
			filter_list)

	filtered_database_names = \
		filter_database_names(
			filtered_classes,
			database_names_dataframe)

	merged_data = \
		merge_dataframes(
			filtered_database_names,
			cable_master_dataframe)

	row_number_data = \
		calculate_row_number(
			merged_data)

	cable_catalogue = \
		filter_by_row_number(
			row_number_data,
			1)

	return \
		cable_catalogue

#
# result = cable_core_catalogue(
# 	'cable_catalogue.csv',
# 	'cable_master.csv',
# 	'database_names.csv')
# print(
# 	result)

# use in calling code - to be deleted
cable_catalogue = \
	create_cable_catalogue(
		cable_catalogue_dataframe=cable_catalogue_dataframe,
		cable_master_dataframe=cable_master_dataframe,
		database_names_dataframe=database_names_dataframe)
