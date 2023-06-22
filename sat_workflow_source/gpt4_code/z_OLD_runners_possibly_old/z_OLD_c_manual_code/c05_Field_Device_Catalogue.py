import pandas


def create_field_device_catalogue_dataframe(
		s_field_device_catalogue_df: pandas.DataFrame,
		vw_database_names_df: pandas.DataFrame) \
		-> pandas.DataFrame:
	# s_field_device_catalogue_df = read_csv_file(
	# 	s_field_device_catalogue_csv)
	# vw_database_names_df = read_csv_file(
	# 	vw_database_names_csv)

	filtered_df = \
		__filter_data(
			s_field_device_catalogue_df,
			vw_database_names_df)

	result_df = \
		__select_columns(
			filtered_df)

	return \
		result_df


# def read_csv_file(
# 		file_name: str) \
# 		-> pd.DataFrame:
# 	return pd.read_csv(
# 		file_name)


def __filter_data(
		dataframe: pandas.DataFrame,
		df_vw_database_names: pandas.DataFrame) \
		-> pandas.DataFrame:
	filtered_dataframe = \
		dataframe[
			(dataframe["Catalogue_RNT"] == 1) &
			(dataframe["Catalogue_Name"] != "") &
			(dataframe["database_name"].isin(
				df_vw_database_names["Database_name"]))
			]

	return \
		filtered_dataframe


def __select_columns(
		dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	columns_to_select = \
		[
			"Catalogue_Name",
			"Left_pin_details",
			"Left_Marking",
			"Right_pin_details",
			"Right_Marking",
			"Tag_Number",
			"Loop_Number",
			"Document_number"
		]

	return \
		dataframe[columns_to_select].drop_duplicates()

# # Usage example
# result = field_device_catalogue(
# 	"s_field_device_catalogue.csv",
# 	"vw_database_names.csv")
# print(
# 	result)
