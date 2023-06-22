import pandas
from pandas import DataFrame


def rename_dataframe_columns_to_lowercase(
        dataframe: DataFrame)\
        -> None:
    """
    Renames all column headings in the given dataframe to lowercase.

    Args:
        dataframe (pandas.DataFrame): The input dataframe.

    Returns:
        pandas.DataFrame: The modified dataframe with lowercase column headings.
    """
    casefold_lowercase_columns = \
        [column.casefold()
         for column
         in dataframe.columns]
    
    dataframe.columns = \
        casefold_lowercase_columns
    
    # return \
    #     dataframe

# # Example usage
# input_dataframe = pd.read_csv('input.csv')  # Replace 'input.csv' with your actual input file path
# modified_dataframe = rename_columns_to_lowercase(input_dataframe)
# print(modified_dataframe)
