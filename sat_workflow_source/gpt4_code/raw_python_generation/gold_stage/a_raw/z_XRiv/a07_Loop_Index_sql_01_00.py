import pandas as pd

def read_csv_to_dataframe(file_path):
    df = pd.read_csv(file_path)
    df = df.assign(**{col: df[col].values for col in df.columns})
    return df

def rename_columns(dataframe, column_mapping):
    return dataframe.rename(columns=column_mapping)

def loop_index(cable_schedule_csv, vw_database_names_csv):
    cable_schedule_df = read_csv_to_dataframe(cable_schedule_csv)
    vw_database_names_df = read_csv_to_dataframe(vw_database_names_csv)
    
    database_names = vw_database_names_df['Database_name'].tolist()
    filtered_cable_schedule = cable_schedule_df[cable_schedule_df['database_name'].isin(database_names)]

    column_mapping = {
        'Cable_Drum_Number': 'Cable Drum Number',
        'Laying_Corner_Point': 'Laying Corner Point',
        'External_document_of_item': 'External document of item',
        'Installed_Length': 'Installed Length',
        'Installation_Date': 'Installation Date',
        'Estimated_Length': 'Estimated Length',
        'Function_text_1': 'Function text 1',
        'Level_of_Installation': 'Level of Installation',
        'Shield_number': 'Shield number',
        'Wire_number': 'Wire number',
        'Cable_Set': 'Cable Set',
        'Wire_type': 'Wire type',
        'Conductor_type': 'Conductor type',
        'Insulating_material': 'Insulating material',
        'Inductance_per_km': 'Inductance Per km',
        'Bending_radius': 'Bending radius',
        'Capacitance_per_km': 'Capacitance Per km',
        'Shield': 'Shield',
        'Rated_voltage_Uo': 'Rated_voltage Uo',
        'Rated_voltage_U': 'Rated_voltage U',
        'Precious_metal_factor_2': 'Precious metal factor 2',
        'Precious_metal_factor_1': 'Precious metal factor 1',
        'Suppliers_article_no': 'Suppliers article number',
        'Mass': 'Mass',
        'Component_description_1': 'Component description 1',
        'Selection_key': 'Selection key',
        'Outside_diameter': 'Outside diameter',
        'Subassembly_information': 'Subassembly information',
        'Rated_temperature': 'Rated temperature',
        'Quantity_unit': 'Quantity unit',
        'Mounting_feature': 'Mounting feature',
        'min_ambient_temperature': 'minimum ambient temperature',
        'Measure_unit_qualifier': 'Measure unit qualifier',
        'max_ambient_temperature': 'maximum ambient temperature',
        'List_price': 'List price',
        'EAN_number': 'EAN number',
        'Body_length': 'Body length'
    }

    renamed_df = rename_columns(filtered_cable_schedule, column_mapping)
    return renamed_df.drop_duplicates()

# Example usage:
result = loop_index('cable_schedule.csv', 'vw_database_names.csv')
print(result)
