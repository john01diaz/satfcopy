from typing import Dict

import pandas


def create_dataframe_gold_c03_cable_core_catalogue(
		s_cable_core_catalogue_dataframe: pandas.DataFrame,
		s_cable_catalogue_number_master_dataframe: pandas.DataFrame,
		s_cable_catalogue_dataframe: pandas.DataFrame,
		vw_database_names_dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	# s_cable_core_catalogue = load_csv(
	# 	s_cable_core_catalogue_csv)
	#
	# s_cable_catalogue_number_master = load_csv(
	# 	s_cable_catalogue_number_master_csv)
	#
	# s_cable_catalogue = load_csv(
	# 	s_cable_catalogue_csv)
	#
	# vw_database_names = load_csv(
	# 	vw_database_names_csv)

	# identical to list in c03_Cable_Catalogue
	filter_list = \
		[
			'Instrumentation',
			'Inst(Shared)',
			'Elec(Shared)'
			]

	# Filter s_cable_catalogue
	filtered_s_cable_catalogue = \
		s_cable_catalogue_dataframe[s_cable_catalogue_dataframe['Class'].isin(
			filter_list)]

	# Note: it was changed by hand: from 'database_name' to 'Database_name'
	join_column_names_1 = \
		{
			'database_name': 'Database_name',
			'Cable_Object_Identifier': 'Cable_Object_Identifier'
		}

	# Merge dataframes
	merged_data = \
		merge_dataframes(
			s_cable_core_catalogue_dataframe,
			s_cable_catalogue_number_master_dataframe,
			join_column_names_1)

	# Note: it was changed by hand: from 'database_name' to 'Database_name'
	join_column_names_2 = \
		{
			'database_name': 'Database_name',
			'Cable_Object_Identifier': 'Object_Identifier'
		}

	merged_data = \
		merge_dataframes(
			merged_data,
			filtered_s_cable_catalogue,
			join_column_names_2)

	# Filter merged_data by database_name
	database_names = \
		vw_database_names_dataframe['Database_name'].unique()

	filtered_merged_data = \
		merged_data[merged_data['database_name'].isin(
			database_names)]

	# Note: I removed 'database_name' from the ranking list: from ['database_name', 'Cable_Object_Identifier'] to ['Cable_Object_Identifier']
	# Add Dense Rank
	filtered_merged_data['RNT'] = \
		filtered_merged_data.groupby(
			'CatalogueNo')['Cable_Object_Identifier'].rank(
			method='dense',
			ascending=True)

	# Filter rows with RNT = 1
	final_data = \
		filtered_merged_data[filtered_merged_data['RNT'] == 1]

	# Note: renamed from 'Description' to 'Description_x'
    # JPr Note: Renamed back
	columns = \
		[
			'CatalogueNo',
			'Description_x',
			'Group_Marking',
			'Group_Marking_Sequence',
			'Core_Markings',
			'Core_Markings_Core_Type'
			]

	# Select columns
	cable_core_catalogue = \
		final_data[
			columns]

	return \
		cable_core_catalogue


# def load_csv(
# 		file_path: str) \
# 		-> pandas.DataFrame:
# 	return pandas.read_csv(
# 		file_path)


def merge_dataframes(
		df1: pandas.DataFrame,
		df2: pandas.DataFrame,
		on_columns: Dict[str, str]) \
		-> pandas.DataFrame:
	return pandas.merge(
		df1,
		df2,
		left_on=list(
			on_columns.keys()),
		right_on=list(
			on_columns.values()))

