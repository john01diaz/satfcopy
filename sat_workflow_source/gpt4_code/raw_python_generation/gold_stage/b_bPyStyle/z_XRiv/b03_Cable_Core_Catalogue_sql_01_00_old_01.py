import pandas as pd

# Constants
CABLE_CLASSES = ['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']
RANK_NUMBER = 1

# Output column order
COLUMN_ORDER = ['CatalogueNo', 'Description', 'Group_Marking',
                'Group_Marking_Sequence', 'Core_Markings', 'Core_Markings_Core_Type']


def load_data_frame(
        df):
    """Load DataFrame from input parameter"""
    return df.copy()


def join_data_frames(
        df1,
        df2,
        join_conditions):
    """Join two DataFrames based on conditions"""
    joined_df = pd.merge(
        df1,
        df2,
        left_on=join_conditions[0],
        right_on=join_conditions[1])
    return joined_df


def filter_data(
        df,
        column,
        values):
    """Filter DataFrame based on conditions"""
    filtered_df = df[df[column].isin(
        values)]
    return filtered_df


def rank_data(
        df,
        partition_column,
        order_columns):
    """Rank data using dense rank over partition"""
    df['RNT'] = df.sort_values(
        order_columns).groupby(
        partition_column).cumcount() + 1
    return df


def select_columns(
        df,
        columns):
    """Select columns from DataFrame in specific order"""
    selected_data = df[columns]
    return selected_data


def cable_core_catalogue_sql_01_00(
        cable_core_catalogue,
        cable_catalogue_number_master,
        cable_catalogue,
        database_names):
    """Main function to process data"""
    
    # Load DataFrame
    cable_core_catalogue = load_data_frame(
        cable_core_catalogue)
    cable_catalogue_number_master = load_data_frame(
        cable_catalogue_number_master)
    cable_catalogue = load_data_frame(
        cable_catalogue)
    
    # Join data
    joined_data = join_data_frames(
        cable_core_catalogue,
        cable_catalogue_number_master,
        [('database_name', 'Cable_Object_Identifier'),
         ('database_name', 'Cable_Object_Identifier')])
    joined_data = join_data_frames(
        joined_data,
        cable_catalogue,
        [('database_name', 'Cable_Object_Identifier'),
         ('database_name', 'Object_Identifier')])
    
    # Filter data
    joined_data = filter_data(
        joined_data,
        'Class',
        CABLE_CLASSES)
    joined_data = filter_data(
        joined_data,
        'database_name',
        database_names['Database_name'].tolist())
    
    # Rank data
    ranked_data = rank_data(
        joined_data,
        'CatalogueNo',
        ['database_name', 'Cable_Object_Identifier'])
    
    # Filter ranked data
    final_data = ranked_data[ranked_data['RNT'] == RANK_NUMBER]
    
    # Select columns in the required order
    final_data = select_columns(
        final_data,
        COLUMN_ORDER)
    
    return final_data
