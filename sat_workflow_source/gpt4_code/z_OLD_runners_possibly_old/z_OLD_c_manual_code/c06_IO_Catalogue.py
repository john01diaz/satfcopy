import pandas

# Define named constants
CATALOGUE_RNT = 1
TERMINALS_MARKING_REPLACE = \
	{
		'+': '',
		'-': ''
	}

VW_DATABASE_NAMES = \
	pandas.read_csv(
		'VW_Database_names.csv')['Database_name'].tolist()


def create_06_io_catalogue_dataframe(
		io_catalogue_dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	# Read in the input CSV file as a_raw Pandas DataFrame
	# io_catalogue = \
	# 	pandas.read_csv(
	# 		io_catalogue_file_path)

	# Rename columns if necessary
	# column_rename = {'old_name': 'new_name', ...}
	# io_catalogue.rename(columns=column_rename, inplace=True)

	# Apply filter conditions
	io_catalogue = \
		io_catalogue_dataframe[
			(io_catalogue_dataframe['Catalogue_RNT'] == CATALOGUE_RNT) &
			(io_catalogue_dataframe['database_name'].isin(
				VW_DATABASE_NAMES))]

	# Replace characters in the 'TerminalsPerMarking' column and convert to integer
	io_catalogue['TerminalsPerMarking'] = \
		io_catalogue['TerminalsPerMarking'].replace(
			TERMINALS_MARKING_REPLACE,
			regex=True).astype(
			int)

	# Sort the DataFrame by 'Model_Number' and 'TerminalsPerMarking'
	io_catalogue = \
		io_catalogue.sort_values(
			by=['Model_Number', 'TerminalsPerMarking'],
			key=lambda
				x: x.astype(
				str).str.zfill(
				20))

	# Return only the selected columns
	columns = \
		[
			'Model_Number',
			'Description',
			'Manufacturer',
			'DescriptionDrawing',
			'Channel',
			'AllowUse',
			'IOType',
			'NoOfPoints',
			'TerminalsPerPointChannel',
			'TerminalsPerMarking'
		]

	io_catalogue = \
		io_catalogue[columns].drop_duplicates()

	return \
		io_catalogue
