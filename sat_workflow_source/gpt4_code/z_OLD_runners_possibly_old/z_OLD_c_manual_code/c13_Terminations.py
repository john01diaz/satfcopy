import pandas
#
# # Constants
# TERMINATIONS_CSV = \
# 	"terminations.csv"
#
# DATABASE_NAMES_CSV = \
# 	"database_names.csv"


def create_terminations_dataframe(
		terminations_dataframe: pandas.DataFrame,
		database_names_dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	# terminations_df = read_csv_file(
	# 	TERMINATIONS_CSV)
	# database_names_df = read_csv_file(
	# 	DATABASE_NAMES_CSV)

	filtered_dataframe = \
		__filter_by_class_and_database(
			terminations_dataframe,
			database_names_dataframe)

	distinct_columns_dataframe = \
		__get_distinct_columns(
			filtered_dataframe)

	# Apply column renames using the RenamedColumns class
	column_rename_dict = \
		{}  # Add any required renames in this dictionary

	renamed_columns = \
		RenamedColumns(
			column_rename_dict)

	terminations_dataframe = \
		renamed_columns.apply(
			distinct_columns_dataframe)

	return \
		terminations_dataframe


# Immutable class for storing the renamed columns
class RenamedColumns:
	def __init__(
			self,
			rename_dict):
		self.rename_dict = \
			rename_dict

	def apply(
			self,
			dataframe):
		return \
			dataframe.rename(
				columns=self.rename_dict)


# def read_csv_file(
# 		file_path,
# 		*args,
# 		**kwargs):
# 	return pandas.read_csv(
# 		file_path,
# 		*args,
# 		**kwargs)


def __filter_by_class_and_database(
		terminations_dataframe: pandas.DataFrame,
		database_names_dataframe: pandas.DataFrame) \
		-> pandas.DataFrame:
	filtered_dataframe = \
		terminations_dataframe[
			terminations_dataframe["Class"].isin(
				["Instrumentation", "Inst(Shared)", "Elec(Shared)"])
		]

	database_name_filtered_dataframe = \
		database_names_dataframe["Database_name"]

	return \
		filtered_dataframe[filtered_dataframe["database_name"].isin(
			database_name_filtered_dataframe)]


def __get_distinct_columns(
		filtered_dataframe):
	subset = \
		[
			"CableNumber",
			"Core_Markings",
			"Parent_Equipment_No",
			"Equipment_No",
			"Marking",
			"Left_Right"
		]

	return \
		filtered_dataframe.drop_duplicates(
			subset=subset)


#
# if __name__ == "__main__":
# 	result = terminations_13()
# 	print(
# 		result)
