import pandas as pd

def create_database_names_view(database_string):
    # Split the input string into a_raw list of database names
    database_names = database_string.split(",")
    
    # Create a_raw pandas dataframe with a_raw column for database names
    df = pd.DataFrame(database_names, columns=['Database_name'])
    
    # Create a_raw temporary view for the dataframe
    df.createOrReplaceTempView("VW_Database_names")
    
    # Return the dataframe
    return df
