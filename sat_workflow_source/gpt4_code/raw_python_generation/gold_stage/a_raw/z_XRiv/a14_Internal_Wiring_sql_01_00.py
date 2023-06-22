import pandas as pd

def read_csv_file(file_name):
    return pd.read_csv(file_name, dtype=str)

def filter_classes(data_frame, classes):
    return data_frame[data_frame['Class'].isin(classes)]

def filter_database_names(data_frame, database_names):
    return data_frame[data_frame['database_name'].isin(database_names)]

def rename_columns(data_frame, column_dict):
    return data_frame.rename(columns=column_dict)

def write_csv_file(data_frame, file_name):
    data_frame.to_csv(file_name, index=False)

def internal_wiring(input_file_name, database_names_file, output_file_name):
    # Constants
    CLASSES = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
    COLUMN_RENAMES = {
        'From_Parent_Equipment_No': 'From_Parent_Equipment_No',
        'From_Compartment': 'From_Compartment',
        'From_Equipment': 'From_Equipment',
        'From_Wire_Link': 'From_Wire_Link',
        'From_Marking': 'From_Marking',
        'From_Left_Right': 'From_Left_Right',
        'To_Parent_Equipment_No': 'To_Parent_Equipment_No',
        'To_Compartment': 'To_Compartment',
        'To_Equipment_No': 'To_Equipment_No',
        'To_Wire_Link': 'To_Wire_Link',
        'To_Marking': 'To_Marking',
        'To_Left_Right': 'To_Left_Right'
    }

    # Read input CSV files into pandas dataframes
    input_data = read_csv_file(input_file_name)
    database_names_data = read_csv_file(database_names_file)

    # Filter data based on specified classes
    filtered_data = filter_classes(input_data, CLASSES)

    # Extract unique database names from the database_names_data dataframe
    unique_database_names = set(database_names_data['Database_name'].unique())

    # Filter data based on unique database names
    filtered_data = filter_database_names(filtered_data, unique_database_names)

    # Select required columns
    selected_columns_data = filtered_data[list(COLUMN_RENAMES.keys())].drop_duplicates()

    # Rename columns if necessary
    renamed_data = rename_columns(selected_columns_data, COLUMN_RENAMES)

    # Write the output to a_raw CSV file
    write_csv_file(renamed_data, output_file_name)

# Usage example
input_file_name = 'input.csv'
database_names_file = 'database_names.csv'
output_file_name = '14_Internal_Wiring.csv'
internal_wiring(input_file_name, database_names_file, output_file_name)
