import pandas as pd
from typing import Dict

def load_csv(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)

def merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, on_columns: Dict[str, str]) -> pd.DataFrame:
    return pd.merge(df1, df2, left_on=list(on_columns.keys()), right_on=list(on_columns.values()))

def cable_core_catalogue(s_cable_core_catalogue_csv: str, s_cable_catalogue_number_master_csv: str, s_cable_catalogue_csv: str, vw_database_names_csv: str) -> pd.DataFrame:
    s_cable_core_catalogue = load_csv(s_cable_core_catalogue_csv)
    s_cable_catalogue_number_master = load_csv(s_cable_catalogue_number_master_csv)
    s_cable_catalogue = load_csv(s_cable_catalogue_csv)
    vw_database_names = load_csv(vw_database_names_csv)

    # Filter s_cable_catalogue
    filtered_s_cable_catalogue = s_cable_catalogue[s_cable_catalogue['Class'].isin(['Instrumentation', 'Inst(Shared)', 'Elec(Shared)'])]

    # Merge dataframes
    merged_data = merge_dataframes(s_cable_core_catalogue, s_cable_catalogue_number_master, {'database_name': 'database_name', 'Cable_Object_Identifier': 'Cable_Object_Identifier'})
    merged_data = merge_dataframes(merged_data, filtered_s_cable_catalogue, {'database_name': 'database_name', 'Cable_Object_Identifier': 'Object_Identifier'})

    # Filter merged_data by database_name
    database_names = vw_database_names['Database_name'].unique()
    filtered_merged_data = merged_data[merged_data['database_name'].isin(database_names)]

    # Add Dense Rank
    filtered_merged_data['RNT'] = filtered_merged_data.groupby('CatalogueNo')['database_name', 'Cable_Object_Identifier'].rank(method='dense', ascending=True)

    # Filter rows with RNT = 1
    final_data = filtered_merged_data[filtered_merged_data['RNT'] == 1]

    # Select columns
    selected_columns = final_data[['CatalogueNo', 'Description', 'Group_Marking', 'Group_Marking_Sequence', 'Core_Markings', 'Core_Markings_Core_Type']]

    return selected_columns
