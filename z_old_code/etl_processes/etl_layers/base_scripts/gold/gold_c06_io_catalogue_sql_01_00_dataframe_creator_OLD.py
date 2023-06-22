import pandas

# Define named constants
CATALOGUE_RNT = \
    1

TERMINALS_MARKING_REPLACE = \
    {
        '+': '',
        '-': ''
    }


# Note: parameter vw_database_names_dataframe was added and VW_DATABASE_NAMES was removed.
def create_dataframe_gold_c06_io_catalogue_sql_01_00(
        io_catalogue_dataframe: pandas.DataFrame,
        vw_database_names_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    database_names = \
        vw_database_names_dataframe['Database_name'].tolist()

    io_catalogue = \
        io_catalogue_dataframe[
            (io_catalogue_dataframe['Catalogue_RNT'] == CATALOGUE_RNT) &
            (io_catalogue_dataframe['database_name'].isin(
                database_names))]

    # Note: added a copy of the main dataframe because it was crashing
    io_catalogue_dataframe = \
        io_catalogue.copy()

    # Note: .replace(TERMINALS_MARKING_REPLACE, regex=True) was changed to replace('\+', '', regex=True) and
    #  .replace('\-', '', regex=True)
    #  and a .str was added to convert the column to string
    #  .astype(int) was remvoed because the column does not have only numbers
    io_catalogue_dataframe['TerminalsPerMarking'] = \
        io_catalogue_dataframe['TerminalsPerMarking'].str.replace(
            '\+', '', regex=True).replace(
            '\-', '', regex=True)

    io_catalogue = \
        io_catalogue.sort_values(
            by=['Model_Number', 'TerminalsPerMarking'],
            key=lambda
                x: x.astype(
                str).str.zfill(
                20))

    columns = \
        [
            'Model_Number',
            'Description',
            'Manufacturer',
            'DescriptionDrawing',
            'Channel',
            'AllowUse',
            'IOType',
            'NoOfPoints',
            'TerminalsPerPointChannel',
            'TerminalsPerMarking'
        ]

    io_catalogue = \
        io_catalogue[columns].drop_duplicates()

    return \
        io_catalogue
