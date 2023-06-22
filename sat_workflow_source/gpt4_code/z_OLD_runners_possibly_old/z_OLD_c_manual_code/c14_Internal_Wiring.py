import pandas


def create_internal_wiring_dataframe(
		s_internal_wiring_dataframe: pandas.DataFrame,
		database_names_dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	# Constants
	CLASSES = \
		[
			'Instrumentation',
			'Inst(Shared)',
			'Elec(Shared)'
		]

	COLUMN_RENAMES = \
		{
			'From_Parent_Equipment_No': 'From_Parent_Equipment_No',
			'From_Compartment': 'From_Compartment',
			'From_Equipment': 'From_Equipment',
			'From_Wire_Link': 'From_Wire_Link',
			'From_Marking': 'From_Marking',
			'From_Left_Right': 'From_Left_Right',
			'To_Parent_Equipment_No': 'To_Parent_Equipment_No',
			'To_Compartment': 'To_Compartment',
			'To_Equipment_No': 'To_Equipment_No',
			'To_Wire_Link': 'To_Wire_Link',
			'To_Marking': 'To_Marking',
			'To_Left_Right': 'To_Left_Right'
		}

	# # Read input CSV files into pandas dataframes
	# input_data = read_csv_file(
	# 	input_file_name)
	# database_names_data = read_csv_file(
	# 	database_names_file)

	# Filter data based on specified classes
	filtered_data = \
		__filter_classes(
			s_internal_wiring_dataframe,
			CLASSES)

	# Extract unique database names from the database_names_data dataframe
	unique_database_names = \
		set(
			database_names_dataframe['Database_name'].unique())

	# Filter data based on unique database names
	filtered_data = \
		__filter_database_names(
			filtered_data,
			unique_database_names)

	# Select required columns
	selected_columns_data = \
		filtered_data[list(
			COLUMN_RENAMES.keys())].drop_duplicates()

	# Rename columns if necessary
	internal_wiring_dataframe = \
		__rename_columns(
			selected_columns_data,
			COLUMN_RENAMES)

	# # Write the output to a_raw CSV file
	# write_csv_file(
	# 	renamed_data,
	# 	output_file_name)

	return \
		internal_wiring_dataframe


# def read_csv_file(
# 		file_name):
# 	return pd.read_csv(
# 		file_name,
# 		dtype=str)


def __filter_classes(
		data_frame,
		classes):
	return \
		data_frame[data_frame['Class'].isin(
			classes)]


def __filter_database_names(
		dataframe,
		database_names):
	return \
		dataframe[dataframe['database_name'].isin(
			database_names)]


def __rename_columns(
		dataframe,
		column_dict):
	return \
		dataframe.rename(
			columns=column_dict)


# def write_csv_file(
# 		data_frame,
# 		file_name):
# 	data_frame.to_csv(
# 		file_name,
# 		index=False)


# # Usage example
# input_file_name = 'input.csv'
# database_names_file = 'database_names.csv'
# output_file_name = '14_Internal_Wiring.csv'
# internal_wiring(
# 	input_file_name,
# 	database_names_file,
# 	output_file_name)
