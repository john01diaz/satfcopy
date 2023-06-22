import pandas as pd
import re

def read_data_from_file(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)

def extract_type(string: str) -> str:
    match = re.match(r'[A-Za-z]+', string)
    return match.group(0) if match else None

def filter_instrument_data(data_frame: pd.DataFrame) -> pd.DataFrame:
    conditions = (
        data_frame['CS_location_full_designation'].isnull()
        & data_frame['loop_element_database_name'].isin(database_names) # Assuming database_names is defined
        & data_frame['CS_device_type'].notnull()
        & data_frame['CS_device_type'].str.strip().ne("")
    )
    return data_frame[conditions].copy()

def get_unique_instrument_types(data_frame: pd.DataFrame) -> pd.DataFrame:
    data_frame['Type'] = data_frame['CS_loop_element_id'].apply(extract_type)
    data_frame['InstrumentType'] = data_frame['CS_device_type'].str.upper().str.strip()
    unique_instrument_types = data_frame[['Type', 'InstrumentType']].drop_duplicates()
    return unique_instrument_types.copy()

def sort_by_type(data_frame: pd.DataFrame) -> pd.DataFrame:
    return data_frame.sort_values(by=['Type']).copy()

def write_data_to_file(data_frame: pd.DataFrame, file_path: str):
    data_frame.to_csv(file_path, index=False)

def process_instrument_data(input_file_path: str, output_file_path: str):
    input_data = read_data_from_file(input_file_path)
    filtered_instrument_data = filter_instrument_data(input_data)
    unique_instrument_types = get_unique_instrument_types(filtered_instrument_data)
    sorted_instrument_types = sort_by_type(unique_instrument_types)
    write_data_to_file(sorted_instrument_types, output_file_path)

def _02_Instrument_Type_Catalogue(input_file: str, output_file: str):
    process_instrument_data(input_file, output_file)

if __name__ == '__main__':
    # Assuming database_names is a_raw list of database names, e.g., ['Database1', 'Database2']
    database_names = ['Database1', 'Database2']
    _02_Instrument_Type_Catalogue('input_file.csv', 'output_file.csv')
