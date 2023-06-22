import pandas


def create_dataframe_gold_c13_terminations_sql_01_00(
        terminations_dataframe: pandas.DataFrame,
        database_names_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    filtered_dataframe = \
        __filter_by_class_and_database(
            terminations_dataframe,
            database_names_dataframe)

    distinct_columns_dataframe = \
        __get_distinct_columns(
            filtered_dataframe)

    column_rename_dict = \
        {}

    renamed_columns = \
        RenamedColumns(
            column_rename_dict)

    terminations_dataframe = \
        renamed_columns.apply(
            distinct_columns_dataframe)

    return \
        terminations_dataframe


class RenamedColumns:
    def __init__(
            self,
            rename_dict):
        self.rename_dict = \
            rename_dict

    def apply(
            self,
            dataframe):
        return \
            dataframe.rename(
                columns=self.rename_dict)


def __filter_by_class_and_database(
        terminations_dataframe: pandas.DataFrame,
        database_names_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    filtered_dataframe = \
        terminations_dataframe[
            terminations_dataframe["Class"].isin(
                ["Instrumentation", "Inst(Shared)", "Elec(Shared)"])
        ]

    database_name_filtered_dataframe = \
        database_names_dataframe["database_name"]

    return \
        filtered_dataframe[filtered_dataframe["database_name"].isin(
            database_name_filtered_dataframe)]


def __get_distinct_columns(
        filtered_dataframe):
    columns_to_keep = \
        [
            "CableNumber",
            "Core_Markings",
            "Parent_Equipment_No",
            "Equipment_No",
            "Marking",
            "Left_Right"
        ]

    # Note: edited by hand the generated steps by GPT-4 because was not creating the expected set of columns
    filtered_dataframe = \
        filtered_dataframe.filter(
            items=columns_to_keep)

    return \
        filtered_dataframe.drop_duplicates()
