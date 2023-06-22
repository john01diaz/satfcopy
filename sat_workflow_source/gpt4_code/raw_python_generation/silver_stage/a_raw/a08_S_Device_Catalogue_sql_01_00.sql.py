from enum import Enum
import pandas
import numpy as np


class DatabaseNames(Enum):
    DATABASE_NAME = 'database_name'


class S_Item_Function_Model(Enum):
    # All enums are defined here


class S_ItemFunction(Enum):
    # All enums are defined here


class S_DeviceCatalogue(Enum):
    # All enums are defined here


def coalesce(*args):
    """
    Returns the first non-null/NaN argument.
    """
    for arg in args:
        if arg is not None and not pandas.isna(arg):
            return arg
    return None


def replace_string(value, to_replace, replacement):
    """
    Replaces a substring in a string with another substring.
    """
    if value is not None and not pandas.isna(value):
        return value.replace(to_replace, replacement)
    return None


def join_dataframes(dataframe1, dataframe2, on_columns):
    """
    Joins two dataframes based on given column names.
    """
    return pandas.merge(dataframe1, dataframe2, how='inner', on=on_columns)


def calculate_description(row):
    """
    Calculates the description based on the logic in the SQL statement.
    """
    return coalesce(
        '-'.join([row[S_ItemFunction.DESCRIPTION.value],
                  row[S_ItemFunction.PRODUCT_KEY.value],
                  row[S_ItemFunction.DOCUMENT_NUMBER.value],
                  replace_string(row[S_ItemFunction.DATABASE_NAME.value], '_2016R3', '')]),
        'RHLDD'
    )


def process_dataframe(itemfunction_dataframe, itemfunctionmodel_dataframe):
    """
    Processes the dataframe based on the SQL statement.
    """
    joined_dataframe = join_dataframes(
        itemfunction_dataframe,
        itemfunctionmodel_dataframe,
        [S_ItemFunction.DATABASE_NAME.value, S_ItemFunction.ITEM_DYNAMIC_CLASS.value,
         S_ItemFunction.ITEM_OBJECT_IDENTIFIER.value, S_ItemFunction.DYNAMIC_CLASS.value,
         S_ItemFunction.OBJECT_IDENTIFIER.value]
    )

    joined_dataframe = joined_dataframe[
        joined_dataframe[S_ItemFunction.TYPE.value].isin(['Device', 'FTA']) &
        joined_dataframe[S_ItemFunction.LOCATION_DESIGNATION.value].notna() &
        (coalesce(joined_dataframe[S_Item_Function_Model.LEFT.value], 0) +
         coalesce(joined_dataframe[S_Item_Function_Model.RIGHT.value], 0)) > 0
    ].copy()

    joined_dataframe[S_DeviceCatalogue.ALLOW_USE.value] = 'TRUE'
    joined_dataframe[S_DeviceCatalogue.DESCRIPTION.value] = joined_dataframe.apply(calculate_description, axis=1)
    joined_dataframe[S_DeviceCatalogue.CATALOGUE_RNT.value] = joined_dataframe.groupby(
        S_Item_Function_Model.MODELNO.value)[S_ItemFunction.ITEM_OBJECT_IDENTIFIER.value].rank(method="first")

    return joined_dataframe
