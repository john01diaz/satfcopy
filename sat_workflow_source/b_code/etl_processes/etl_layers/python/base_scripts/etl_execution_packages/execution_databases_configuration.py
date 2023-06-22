import pandas

from sat_workflow_source.b_code.etl_schemas.common.database_names import DatabaseNames


# TODO - work on this later
# TODO - use parameter in config database rather than default ('R_2016R3')
def create_database_names_view(
		input_tables: dict,
		database_string: str='R_2016R3') \
		-> pandas.DataFrame:
	# Split the input string into a_raw list of database names
	database_names = \
		database_string.split(",")

	# Create a_raw pandas dataframe with a_raw column for database names
	dataframe = \
		pandas.DataFrame(
			database_names,
			columns=[DatabaseNames.DATABASE_NAME.value])

	# Create a_raw temporary view for the dataframe
	# Note: it was edited by hand because it's not right
	# df.createOrReplaceTempView(
	# 	"VW_Database_names")

	# Return the dataframe
	return \
		dataframe
