from pandas import DataFrame


def migrate_table_to_bclearer_data_policy(
        table: DataFrame) \
        -> DataFrame:
    cleaned_table = \
        table.copy(
            deep=True)

    # cleaned_table = \
    #     table.replace(
    #         nan,
    #         B_DEFAULT_ISNULL,
    #         regex=True)

    return \
        cleaned_table
