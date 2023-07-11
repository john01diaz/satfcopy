from pandas import DataFrame


def migrate_table_from_bclearer_to_aveva_data_policy(
        table: DataFrame) \
        -> DataFrame:
    cleaned_table = \
        table.copy(
            deep=True)

    # cleaned_table = \
    #     table.replace(
    #         'null',
    #         '',
    #         regex=True)

    return \
        cleaned_table
