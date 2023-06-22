import pandas as pd

def instrument_index(input_file: str) -> pd.DataFrame:
    # Read input CSV file and create an immutable DataFrame
    input_df = pd.read_csv(input_file)
    
    # Filter the DataFrame based on the conditions
    filtered_df = input_df[
        (input_df['Catalogue_RNT'] == 1) &
        (input_df['Class'].isin(['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']))
    ]
    
    # Define the required columns
    required_columns = [
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
    distinct_df = filtered_df[required_columns].drop_duplicates()
    
    # Return the resulting DataFrame
    return distinct_df

# Assuming a "vw_database_names.csv" file is available with "Database_name" column
vw_database_names = pd.read_csv("vw_database_names.csv")
database_names = vw_database_names["Database_name"]

# Assuming the "s_device_catalogue.csv" file is available
s_device_catalogue = pd.read_csv("s_device_catalogue.csv")

# Filter the s_device_catalogue DataFrame by database names from vw_database_names DataFrame
s_device_catalogue_filtered = s_device_catalogue[s_device_catalogue["database_name"].isin(database_names)]

# Save the filtered DataFrame to a new CSV file
s_device_catalogue_filtered.to_csv("s_device_catalogue_filtered.csv", index=False)

# Call the '08_Instrument_Index' method
result_df = instrument_index("s_device_catalogue_filtered.csv")
