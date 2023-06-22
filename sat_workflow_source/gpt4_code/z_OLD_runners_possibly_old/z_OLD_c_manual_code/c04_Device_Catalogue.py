import pandas


def create_device_catalogue_dataframe(
		s_device_catalogue_dataframe: pandas.DataFrame,
		database_names_dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	# Read input CSV file and create an immutable DataFrame
	# input_df = pandas.read_csv(
	# 	input_file)

	# Filter the DataFrame based on the conditions
	filtered_df = \
		s_device_catalogue_dataframe[
			(s_device_catalogue_dataframe['Catalogue_RNT'] == 1) &
			(s_device_catalogue_dataframe['Class'].isin(
				['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']))]

	filtered_database_names = \
		__filter_database_names(
			filtered_df,
			database_names_dataframe)


	# Define the required columns
	required_columns = \
		[
			"AllowUse",
			"Type",
			"Description",
			"Manufacturer",
			"ModelNo",
			"Class",
			"Left",
			"Right",
			"Left_Marking",
			"Right_Marking",
			"Symbol_name",
			"Product_Key",
			"Loop_Number",
			"Tag_Number",
			"Document_Number"
		]

	# Select distinct rows from the filtered DataFrame
	device_catalogue_dataframe = \
		filtered_database_names[required_columns].drop_duplicates()

	# Return the resulting DataFrame
	return \
		device_catalogue_dataframe


# # Assuming a "vw_database_names.csv" file is available with "Database_name" column
# vw_database_names = pandas.read_csv(
# 	"vw_database_names.csv")
# database_names = vw_database_names["Database_name"]
#
# # Assuming the "s_device_catalogue.csv" file is available
# s_device_catalogue = pandas.read_csv(
# 	"s_device_catalogue.csv")

# Filter the s_device_catalogue DataFrame by database names from vw_database_names DataFrame
# s_device_catalogue_filtered = \
# 	s_device_catalogue[s_device_catalogue["database_name"].isin(
# 		database_names)]

def __filter_database_names(
		dataframe: pandas.DataFrame,
		database_names: pandas.DataFrame) \
		-> pandas.DataFrame:
	return dataframe[dataframe['database_name'].isin(
		database_names['Database_name'])]

# # Save the filtered DataFrame to a new CSV file
# s_device_catalogue_filtered.to_csv(
# 	"s_device_catalogue_filtered.csv",
# 	index=False)
#
# # Call the '08_Instrument_Index' method
# result_df = create_dataframe_gold_c04_device_catalogue_sql_01_00(
# 	"s_device_catalogue_filtered.csv")
