import re
import pandas


def create_instrument_type_catalogue_dataframe(
		cs_layer_loop_loop_elements_dataframe: pandas.DataFrame,
		database_names: pandas.DataFrame) \
		-> pandas.DataFrame:
	# input_data = read_data_from_file(
	# 	input_file_path)
	filtered_instrument_data = \
		__filter_instrument_data(
			cs_layer_loop_loop_elements_dataframe,
			database_names)

	unique_instrument_types = \
		__get_unique_instrument_types(
			filtered_instrument_data)

	instrument_type_catalogue_dataframe = \
		__sort_by_type(
			unique_instrument_types)

	# write_data_to_file(
	# 	sorted_instrument_types,
	# 	output_file_path)

	return \
		instrument_type_catalogue_dataframe


# def read_data_from_file(
# 		file_path: str) -> pd.DataFrame:
# 	return pd.read_csv(
# 		file_path)


def __extract_type(
		string: str) \
		-> str:
	match = \
		re.match(
			r'[A-Za-z]+',
			string)

	return \
		match.group(
			0) if match else None


def __filter_instrument_data(
		dataframe: pandas.DataFrame,
		database_names: pandas.DataFrame) \
		-> pandas.DataFrame:
	conditions = (
			dataframe['CS_location_full_designation'].isnull()
			& dataframe['loop_element_database_name'].isin(
				database_names['Database_name'])  # Assuming database_names is defined
			& dataframe['CS_device_type'].notnull()
			& dataframe['CS_device_type'].str.strip().ne(""))

	return \
		dataframe[conditions].copy()


def __get_unique_instrument_types(
		dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	dataframe['Type'] = \
		dataframe['CS_loop_element_id'].apply(
			__extract_type)

	dataframe['InstrumentType'] = \
		dataframe['CS_device_type'].str.upper().str.strip()

	unique_instrument_types = \
		dataframe[['Type', 'InstrumentType']].drop_duplicates()

	return \
		unique_instrument_types.copy()


def __sort_by_type(
		dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	return \
		dataframe.sort_values(
			by=['Type']).copy()


# def write_data_to_file(
# 		dataframe: pandas.DataFrame,
# 		file_path: str):
# 	dataframe.to_csv(
# 		file_path,
# 		index=False)


# def _02_Instrument_Type_Catalogue(
# 		input_file: str,
# 		output_file: str):
# 	process_instrument_data(
# 		input_file,
# 		output_file)
#
#
# if __name__ == '__main__':
# 	# Assuming database_names is a_raw list of database names, e.g., ['Database1', 'Database2']
# 	database_names = ['Database1', 'Database2']
# 	_02_Instrument_Type_Catalogue(
# 		'input_file.csv',
# 		'output_file.csv')
