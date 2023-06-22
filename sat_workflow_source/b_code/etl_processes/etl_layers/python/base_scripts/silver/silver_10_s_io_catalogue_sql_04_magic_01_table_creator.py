from pandas import DataFrame


def create_10_s_io_catalogue_sql_04_magic_01_table(
        VW_IO_CATALOGUE = DataFrame) \
        -> DataFrame:
    
    database_name_value = \
        "R_2016R3"
    
    S_IO_Catalogue = \
        VW_IO_CATALOGUE
    [VW_IO_CATALOGUE['database_name'] == database_name_value]
    
    return \
        S_IO_Catalogue
