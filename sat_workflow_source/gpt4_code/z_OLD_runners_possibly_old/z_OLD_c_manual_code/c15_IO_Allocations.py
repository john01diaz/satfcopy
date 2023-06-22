import pandas


def create_io_allocations_dataframe(
		s_io_allocations_dataframe: pandas.DataFrame,
		database_names_dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	# io_allocations_csv = 'io_allocations.csv'
	# database_names_csv = 'database_names.csv'

	columns_to_select = \
		[
			'Tag_Number',
			'Parent_Equipment_No',
			'IOType',
			'Equipment_No',
			'CatalogueNo',
			'ChannelNumber'
		]

	class_values = \
		[
			'Instrumentation',
			'Inst(Shared)',
			'Elec(Shared)'
		]

	# io_allocations_df = read_csv(
	# 	io_allocations_csv)
	# database_names_df = read_csv(
	# 	database_names_csv)

	filtered_dataframe = \
		filter_class_and_database(
			s_io_allocations_dataframe,
			class_values,
			database_names_dataframe)

	# Rename columns if needed (use a_raw dictionary)
	# rename_dict = {'old_col_name': 'new_col_name'}
	# renamed_df = rename_columns(filtered_df, rename_dict)

	io_allocations_dataframe = \
		select_distinct_columns(
			filtered_dataframe,
			columns_to_select)

	# output_file_path = '15_IO_Allocations_output.csv'
	# save_to_csv(
	# 	distinct_df,
	# 	output_file_path)

	return \
		io_allocations_dataframe


# def read_csv(
# 		file_path):
# 	return pd.read_csv(
# 		file_path)


def rename_columns(
		dataframe,
		rename_dict):
	return \
		dataframe.rename(
			columns=rename_dict)


def filter_class_and_database(
		dataframe,
		class_values,
		databases_df):
	filtered_by_class = \
		dataframe[dataframe['Class'].isin(
			class_values)]

	database_names = \
		databases_df['Database_name'].tolist()

	filtered_by_database = \
		filtered_by_class[filtered_by_class['database_name'].isin(
			database_names)]

	return \
		filtered_by_database


def select_distinct_columns(
		df,
		columns):
	return \
		df[columns].drop_duplicates()


# def save_to_csv(
# 		df,
# 		output_file_path):
# 	df.to_csv(
# 		output_file_path,
# 		index=False)
#
#
# if __name__ == '__main__':
# 	main()
