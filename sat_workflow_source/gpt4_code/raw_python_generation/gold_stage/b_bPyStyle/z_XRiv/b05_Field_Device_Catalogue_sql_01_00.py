import pandas as pd


def read_csv_file(
		file_name: str) -> pd.DataFrame:
	return pd.read_csv(
		file_name)


def filter_data(
		df: pd.DataFrame,
		df_vw_database_names: pd.DataFrame) -> pd.DataFrame:
	filtered_df = df[
		(df["Catalogue_RNT"] == 1) &
		(df["Catalogue_Name"] != "") &
		(df["database_name"].isin(
			df_vw_database_names["Database_name"]))
		]
	return filtered_df


def select_columns(
		df: pd.DataFrame) -> pd.DataFrame:
	columns_to_select = [
		"Catalogue_Name",
		"Left_pin_details",
		"Left_Marking",
		"Right_pin_details",
		"Right_Marking",
		"Tag_Number",
		"Loop_Number",
		"Document_number"
		]
	return df[columns_to_select].drop_duplicates()


def field_device_catalogue(
		s_field_device_catalogue_csv: str,
		vw_database_names_csv: str) -> pd.DataFrame:
	s_field_device_catalogue_df = read_csv_file(
		s_field_device_catalogue_csv)
	vw_database_names_df = read_csv_file(
		vw_database_names_csv)

	filtered_df = filter_data(
		s_field_device_catalogue_df,
		vw_database_names_df)

	result_df = select_columns(
		filtered_df)

	return result_df


# Usage example
result = field_device_catalogue(
	"s_field_device_catalogue.csv",
	"vw_database_names.csv")
print(
	result)
