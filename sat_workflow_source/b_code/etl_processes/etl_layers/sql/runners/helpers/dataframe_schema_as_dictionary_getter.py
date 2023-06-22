# TODO: Create a class BSqlSchemas with a dictionary and a method to return/export the SQL text file
from pandas import DataFrame


def get_dataframe_schema_as_dictionary(
        dataframe: DataFrame) \
        -> dict:
    dataframe_schema = \
        dataframe.dtypes.apply(
            lambda x: x.name).to_dict()

    dataframe_schema_table_as_dictionary = \
        dict()

    for column_name, datatype in dataframe_schema.items():
        dataframe_schema_table_as_dictionary[len(dataframe_schema_table_as_dictionary)] = \
            {
                'column_names': column_name,
                'datatypes': datatype
            }

    return \
        dataframe_schema_table_as_dictionary
