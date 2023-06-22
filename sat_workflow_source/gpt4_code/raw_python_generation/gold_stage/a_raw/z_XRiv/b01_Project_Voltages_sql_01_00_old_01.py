import pandas as pd


def read_data_from_file(
		file_path: str) -> pd.DataFrame:
	return pd.read_csv(
		file_path)


def filter_voltage_data(
		data_frame: pd.DataFrame) -> pd.DataFrame:
	conditions = (
			data_frame['loop_element_cs_voltage_type'].notnull()
			& (data_frame['loop_element_cs_voltage_type'] != '_empty_')
			& (data_frame['loop_element_cs_voltage_type'] != " ")
			& (data_frame['CS_voltage'] != '+0')
	)
	return data_frame[conditions].copy()


def get_unique_voltage_data(
		data_frame: pd.DataFrame) -> pd.DataFrame:
	return data_frame[['loop_element_cs_voltage_type', 'CS_voltage']].drop_duplicates().copy()


def rename_data_frame_columns(
		data_frame: pd.DataFrame,
		column_dictionary: dict) -> pd.DataFrame:
	return data_frame.rename(
		columns=column_dictionary).copy()


def write_data_to_file(
		data_frame: pd.DataFrame,
		file_path: str):
	data_frame.to_csv(
		file_path,
		index=False)


def process_voltage_data(
		input_file_path: str,
		output_file_path: str):
	input_data = read_data_from_file(
		input_file_path)
	filtered_voltage_data = filter_voltage_data(
		input_data)
	unique_voltage_data = get_unique_voltage_data(
		filtered_voltage_data)
	column_name_mapping = {
		'loop_element_cs_voltage_type': 'Voltage_Type',
		'CS_voltage': 'Voltage'
		}
	unique_voltage_data_renamed = rename_data_frame_columns(
		unique_voltage_data,
		column_name_mapping)
	write_data_to_file(
		unique_voltage_data_renamed,
		output_file_path)


def project_voltages(
		input_file: str,
		output_file: str):
	process_voltage_data(
		input_file,
		output_file)


if __name__ == '__main__':
	project_voltages(
		'input_file.csv',
		'output_file.csv')
