import pandas


def create_process_voltages_dataframe(
		cs_layer_loop_loop_elements_dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	# input_data = read_data_from_file(
	# 	input_file_path)
	# filtered_voltage_data = filter_voltage_data(
	# 	input_data)
	unique_voltage_data = \
		__get_unique_voltage_data(
			cs_layer_loop_loop_elements_dataframe)

	column_name_mapping = \
		{
			'loop_element_cs_voltage_type': 'Voltage_Type',
			'CS_voltage': 'Voltage'
			}

	process_voltages_dataframe = \
		__rename_dataframe_columns(
			unique_voltage_data,
			column_name_mapping)

	# write_data_to_file(
	# 	unique_voltage_data_renamed,
	# 	output_file_path)

	return \
		process_voltages_dataframe


# def read_data_from_file(
# 		file_path: str) -> pd.DataFrame:
# 	return pd.read_csv(
# 		file_path)


def __filter_voltage_data(
		dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	conditions = (
		dataframe['loop_element_cs_voltage_type'].notnull()
		& (dataframe['loop_element_cs_voltage_type'] != '_empty_')
		& (dataframe['loop_element_cs_voltage_type'] != " ")
		& (dataframe['CS_voltage'] != '+0'))

	return \
		dataframe[conditions].copy()


def __get_unique_voltage_data(
		dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	return \
		dataframe[['loop_element_cs_voltage_type', 'CS_voltage']].drop_duplicates().copy()


def __rename_dataframe_columns(
		dataframe: pandas.DataFrame,
		column_dictionary: dict) \
		-> pandas.DataFrame:
	return \
		dataframe.rename(
			columns=column_dictionary).copy()


# def write_data_to_file(
# 		data_frame: pandas.DataFrame,
# 		file_path: str):
# 	data_frame.to_csv(
# 		file_path,
# 		index=False)


# def project_voltages(
# 		input_file: str,
# 		output_file: str):
# 	process_voltage_data(
# 		input_file,
# 		output_file)
#
#
# if __name__ == '__main__':
# 	project_voltages(
# 		'input_file.csv',
# 		'output_file.csv')
