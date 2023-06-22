import pandas as pd

# Constants
CABLE_CLASSES = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']

# List of columns to select in the final output
COLUMN_ORDER = [
    'CatalogueNo', 'Manufacturer', 'Class', 'Description', 'GroupType', 'NoOfGroups',
    'Armoured', 'OAScr', 'GroupScr', 'EarthCore', 'Voltage', 'Size', 'Earth_Core_Size',
    'OD', 'Material', 'Colour1', 'Colour2', 'AllowUse', 'DrumLength', 'LineType',
    'LineTypeColor', 'LineTypeWidth', 'LineTypeArrowHead', 'Remarks'
    ]


def load_data_frame(
        data_frame):
    """Load DataFrame from input parameter"""
    return data_frame.copy()


def join_data_frames(
        df1,
        df2,
        join_conditions):
    """Join two DataFrames"""
    joined_df = pd.merge(
        df1,
        df2,
        left_on=join_conditions[0],
        right_on=join_conditions[1])
    return joined_df


def filter_data(
        data_frame,
        column,
        values):
    """Filter DataFrame based on conditions"""
    filtered_data_frame = data_frame[data_frame[column].isin(
        values)]
    return filtered_data_frame


def rank_data(
        data_frame,
        partition_column,
        order_column):
    """Rank data using row number over partition"""
    data_frame['RNT'] = data_frame.sort_values(
        order_column).groupby(
        partition_column).cumcount() + 1
    return data_frame


def select_columns(
        data_frame,
        columns):
    """Select columns from DataFrame in specific order"""
    selected_data = data_frame[columns]
    return selected_data


def cable_catalogue_sql_01_00(
        cable_catalogue,
        cable_catalogue_number_master,
        database_names):
    """Main function to process data"""
    
    # Load DataFrame
    cable_catalogue = load_data_frame(
        cable_catalogue)
    cable_catalogue_number_master = load_data_frame(
        cable_catalogue_number_master)
    
    # Filter data
    cable_catalogue = filter_data(
        cable_catalogue,
        'Class',
        CABLE_CLASSES)
    cable_catalogue = filter_data(
        cable_catalogue,
        'database_name',
        database_names['Database_name'].tolist())
    
    # Join data
    joined_data = join_data_frames(
        cable_catalogue,
        cable_catalogue_number_master,
        [('database_name', 'Object_Identifier'), ('database_name', 'Cable_Object_Identifier')])
    
    # Rank data
    ranked_data = rank_data(
        joined_data,
        'CatalogueNo',
        'Cable_Object_Identifier')
    
    # Filter ranked data
    final_data = ranked_data[ranked_data['RNT'] == 1]
    
    # Select columns in the required order
    final_data = select_columns(
        final_data,
        COLUMN_ORDER)
    
    return final_data
