import pandas
import pandas as pd


# def read_csv_file(
# 		file_path):
# 	return pd.read_csv(
# 		file_path)


def create_major_equipments_dataframe(
		database_names_dataframe: pandas.DataFrame,
		major_equipments_dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	# Filter major_equipments_df based on database_name in database_names_df
	filtered_major_equipments = \
		major_equipments_dataframe[major_equipments_dataframe['database_name'].isin(
			database_names_dataframe['Database_name'])]

	# Select the required columns and apply renaming if needed
	column_mapping = \
		{
			'Area': 'Area',
			'Parent_Equipment_No': 'Parent_Equipment_No',
			'Description': 'Description',
			'EquipmentType': 'EquipmentType',
			'VendorSupplied': 'VendorSupplied',
			'DwgRequired': 'DwgRequired',
			'Status': 'Status',
			'AreaPath': 'AreaPath',
			'Type': 'Type',
			'Designation': 'Designation',
			'Comment': 'Comment',
			'Installation_site': 'Installation site',
			'Category': 'Category'
		}

	major_equipments_dataframe = \
		filtered_major_equipments[column_mapping.keys()].rename(
			columns=column_mapping)

	# Make the DataFrame immutable
	major_equipments_dataframe.set_flags(
		writeable=False)

	return \
		major_equipments_dataframe


# def main():
# 	database_names_file = 'path/to/database_names.csv'
# 	major_equipments_file = 'path/to/major_equipments.csv'
#
# 	database_names_df = read_csv_file(
# 		database_names_file)
# 	major_equipments_df = read_csv_file(
# 		major_equipments_file)
#
# 	result_df = major_equipments(
# 		database_names_df,
# 		major_equipments_df)
# 	print(
# 		result_df)
#
#
# if __name__ == '__main__':
# 	main()
