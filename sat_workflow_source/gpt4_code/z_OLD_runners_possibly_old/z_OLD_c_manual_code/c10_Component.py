import pandas


def create_10_component_dataframe(
		s_component_dataframe: pandas.DataFrame,
		vw_database_names_dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	# s_component_df = load_csv(
	# 	s_component_csv)
	# vw_database_names_df = load_csv(
	# 	vw_database_names_csv)

	filtered_datafframe = \
		__filter_class_and_database_name(
			s_component_dataframe,
			vw_database_names_dataframe)

	component_dataframe = \
		__add_row_number(
			filtered_datafframe)

	# if column_rename_dict:
	# 	component_dataframe = \
	# 		__rename_columns(
	# 			component_dataframe,
	# 			column_rename_dict)

	return \
		component_dataframe


# def load_csv(
# 		filename: str) -> pd.DataFrame:
# 	return pd.read_csv(
# 		filename)


def __filter_class_and_database_name(
		s_component_dataframe: pandas.DataFrame,
		vw_database_names_dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	filtered_df = \
		s_component_dataframe[(s_component_dataframe['Class'].isin(
		['Instrumentation', 'Inst(Shared)', 'Elec(Shared)'])) & (s_component_dataframe['database_name'].isin(vw_database_names_dataframe['Database_name']))].copy()

	return \
		filtered_df


def __add_row_number(
		filtered_dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	filtered_dataframe['Sequence'] = \
		filtered_dataframe.groupby(['Parent_Equipment_No', 'DinRail']).cumcount() + 1

	return \
		filtered_dataframe
#
#
# def __rename_columns(
# 		dataframe: pandas.DataFrame,
# 		rename_dict: dict) \
# 		-> pandas.DataFrame:
# 	return \
# 		dataframe.rename(
# 			columns=rename_dict)
