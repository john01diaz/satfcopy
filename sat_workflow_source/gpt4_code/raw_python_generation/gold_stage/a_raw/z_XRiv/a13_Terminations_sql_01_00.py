import pandas as pd

# Constants
TERMINATIONS_CSV = "terminations.csv"
DATABASE_NAMES_CSV = "database_names.csv"

# Immutable class for storing the renamed columns
class RenamedColumns:
    def __init__(self, rename_dict):
        self.rename_dict = rename_dict

    def apply(self, dataframe):
        return dataframe.rename(columns=self.rename_dict)


def read_csv_file(file_path, *args, **kwargs):
    return pd.read_csv(file_path, *args, **kwargs)


def filter_by_class_and_database(terminations_df, database_names_df):
    filtered_df = terminations_df[
        terminations_df["Class"].isin(["Instrumentation", "Inst(Shared)", "Elec(Shared)"])
    ]
    database_name_filtered_df = database_names_df["Database_name"]

    return filtered_df[filtered_df["database_name"].isin(database_name_filtered_df)]


def get_distinct_columns(filtered_df):
    return filtered_df.drop_duplicates(
        subset=[
            "CableNumber",
            "Core_Markings",
            "Parent_Equipment_No",
            "Equipment_No",
            "Marking",
            "Left_Right",
        ]
    )


def terminations_13():
    terminations_df = read_csv_file(TERMINATIONS_CSV)
    database_names_df = read_csv_file(DATABASE_NAMES_CSV)

    filtered_df = filter_by_class_and_database(terminations_df, database_names_df)
    distinct_columns_df = get_distinct_columns(filtered_df)

    # Apply column renames using the RenamedColumns class
    column_rename_dict = {}  # Add any required renames in this dictionary
    renamed_columns = RenamedColumns(column_rename_dict)
    final_df = renamed_columns.apply(distinct_columns_df)

    return final_df


if __name__ == "__main__":
    result = terminations_13()
    print(result)
