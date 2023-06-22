import pandas as pd


def load_csv(
		filename: str) -> pd.DataFrame:
	return pd.read_csv(
		filename)


def filter_class_and_database_name(
		s_component_df: pd.DataFrame,
		vw_database_names_df: pd.DataFrame) -> pd.DataFrame:
	filtered_df = s_component_df[(s_component_df['Class'].isin(
		['Instrumentation', 'Inst(Shared)', 'Elec(Shared)'])) &
	                             (s_component_df['database_name'].isin(
		                             vw_database_names_df['Database_name']))].copy()
	return filtered_df


def add_row_number(
		filtered_df: pd.DataFrame) -> pd.DataFrame:
	filtered_df['Sequence'] = filtered_df.groupby(
		['Parent_Equipment_No', 'DinRail']).cumcount() + 1
	return filtered_df


def rename_columns(
		df: pd.DataFrame,
		rename_dict: dict) -> pd.DataFrame:
	return df.rename(
		columns=rename_dict)


def ten_component(
		s_component_csv: str,
		vw_database_names_csv: str,
		column_rename_dict: dict = None) -> pd.DataFrame:
	s_component_df = load_csv(
		s_component_csv)
	vw_database_names_df = load_csv(
		vw_database_names_csv)

	filtered_df = filter_class_and_database_name(
		s_component_df,
		vw_database_names_df)
	result_df = add_row_number(
		filtered_df)

	if column_rename_dict:
		result_df = rename_columns(
			result_df,
			column_rename_dict)

	return result_df
