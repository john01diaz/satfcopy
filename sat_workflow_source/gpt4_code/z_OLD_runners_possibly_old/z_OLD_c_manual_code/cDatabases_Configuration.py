import pandas as pd

# Create or replace temp view VW_Database_names
# As
# Select
# explode(Split('R_2016R3',',')) as Database_name
# TODO - work on this later
def create_database_names_view(
		database_string):
	# Split the input string into a_raw list of database names
	database_names = database_string.split(
		",")

	# Create a_raw pandas dataframe with a_raw column for database names
	df = pd.DataFrame(
		database_names,
		columns=['Database_name'])

	# Create a_raw temporary view for the dataframe
	df.createOrReplaceTempView(
		"VW_Database_names")

	# Return the dataframe
	return df
