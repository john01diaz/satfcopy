from typing import (
	Tuple)

import pandas


def create_instrument_index_dataframe(
		database_names_dataframe: pandas.DataFrame,
		cable_schedule_dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	# database_names = \
	# 	get_database_names(
	# 		database_names_csv)

	cable_schedule_dataframe = \
		get_cable_schedule_data(
			cable_schedule_dataframe,
			database_names_dataframe)

	# # Rename columns using a_raw dictionary if required
	# column_renames = {
	# 	# 'Original column name': 'New column name',
	# 	}
	#
	# if column_renames:
	# 	cable_schedule_dataframe = cable_schedule_dataframe.rename(
	# 		columns=column_renames)

	# Make the DataFrame immutable
	cable_schedule_dataframe = \
		cable_schedule_dataframe.set_flags(
			writeable=False)

	return \
		cable_schedule_dataframe


# def get_database_names(
# 		database_names_csv: str) -> Tuple[str]:
# 	database_names_df = pandas.read_csv(
# 		database_names_csv)
# 	database_names = tuple(
# 		database_names_df['Database_name'].unique())
# 	return database_names


def get_cable_schedule_data(
		cable_schedule_dataframe: pandas.DataFrame,
		database_dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	# cable_schedule_data = pandas.read_csv(
	# 	cable_schedule_csv)

	# Filter the data based on the database_names
	cable_schedule_data = \
		cable_schedule_dataframe[cable_schedule_dataframe['database_name'].isin(
		database_dataframe['Database_name'])]

	# Remove duplicate rows
	cable_schedule_data = \
		cable_schedule_data.drop_duplicates()

	return \
		cable_schedule_data


# Usage example:
# database_names_csv = "path/to/your/database_names.csv"
# cable_schedule_csv = "path/to/your/cable_schedule.csv"
#
# instrument_index = get_instrument_index(
# 	database_names_csv,
# 	cable_schedule_csv)
# print(
# 	instrument_index.head())
