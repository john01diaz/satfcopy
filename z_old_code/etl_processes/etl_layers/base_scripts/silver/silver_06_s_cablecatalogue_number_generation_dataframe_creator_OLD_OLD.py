import pandas
from scipy.stats import rankdata

# Defining Constants
REPLACEMENT_MAPPING = ['% GRAU MMA', ' BLAU MMA', ' BLAU', ' GRAU', ' ROT']
CONSTANT_PREFIX = 'SHRH_CABLE_'
YEAR_ADDED = 2000

# Column Name Constants with Table Prefix
S_CableCoreCatalogue_DATABASE_NAME = 'database_name'
S_CableCoreCatalogue_CABLE_OBJECT_IDENTIFIER = 'cable_object_identifier'
S_CableCoreCatalogue_CORE_MARKINGS = 'core_markings'

S_CableCatalogue_DATABASE_NAME = 'database_name'
# note: the case in the sql is different from the parquet
S_CableCatalogue_DESCRIPTION = 'description'
S_CableCatalogue_OBJECT_IDENTIFIER = 'object_identifier'

TEMP_CATALOGUENO_CATALOGUE_NO = 'catalogueno'
TEMP_CATALOGUENO_DESCRIPTION = 'description'
TEMP_CATALOGUENO_DATABASE_NAME = 'database_name'
TEMP_CATALOGUENO_CABLE_OBJECT_IDENTIFIER = 'cable_object_identifier'
TEMP_CATALOGUENO_MARKINGS = 'markings'


# Function for cleaning description
def clean_description(
        description):
    for word in REPLACEMENT_MAPPING:
        description = description.replace(
            word,
            '').strip()
    return description


# Function for ranking
def dense_rank(
        order):
    return rankdata(
        order,
        method='dense')


# Function to concatenate with constant prefix and rank
def create_catalogue_no(
        rank):
    return CONSTANT_PREFIX + str(
        YEAR_ADDED + rank)


def create_silver_06_s_cablecatalogue_number_generation_dataframe(
        s_cablecorecatalogue_dataframe,
        s_cablecatalogue_dataframe):
    
    # Filtering out null records
    s_cablecorecatalogue_dataframe = s_cablecorecatalogue_dataframe[
        s_cablecorecatalogue_dataframe[S_CableCoreCatalogue_CABLE_OBJECT_IDENTIFIER].notnull()].copy()
    
    # Joining tables on 'database_name' and 'Cable_object_identifier'
    joined_dataframe = pandas.merge(
        s_cablecorecatalogue_dataframe,
        s_cablecatalogue_dataframe,
        left_on=[S_CableCoreCatalogue_DATABASE_NAME, S_CableCoreCatalogue_CABLE_OBJECT_IDENTIFIER],
        right_on=[S_CableCatalogue_DATABASE_NAME, S_CableCatalogue_OBJECT_IDENTIFIER],
        how='inner')
    
    # Dropping duplicates
    joined_dataframe = joined_dataframe.drop_duplicates()
    
    # Cleaning description
    joined_dataframe[S_CableCatalogue_DESCRIPTION] = joined_dataframe[S_CableCatalogue_DESCRIPTION].apply(
        clean_description)
    
    # Grouping and creating 'Markings' column
    grouped_dataframe = joined_dataframe.groupby(
        [S_CableCoreCatalogue_DATABASE_NAME, S_CableCoreCatalogue_CABLE_OBJECT_IDENTIFIER])[
        S_CableCoreCatalogue_CORE_MARKINGS].apply(
        list).reset_index(
        name=TEMP_CATALOGUENO_MARKINGS)
    
    # Merging original dataframe with grouped one
    silver_06_s_cablecatalogue_number_generation_dataframe = pandas.merge(
        joined_dataframe,
        grouped_dataframe,
        on=[S_CableCoreCatalogue_DATABASE_NAME, S_CableCoreCatalogue_CABLE_OBJECT_IDENTIFIER])
    
    # Creating 'CatalogueNo' column
    silver_06_s_cablecatalogue_number_generation_dataframe[TEMP_CATALOGUENO_CATALOGUE_NO] = \
    silver_06_s_cablecatalogue_number_generation_dataframe[
        [TEMP_CATALOGUENO_DESCRIPTION, TEMP_CATALOGUENO_MARKINGS]].apply(
        dense_rank).apply(
        create_catalogue_no)
    
    # Selecting required columns
    silver_06_s_cablecatalogue_number_generation_dataframe = silver_06_s_cablecatalogue_number_generation_dataframe[
        [TEMP_CATALOGUENO_CATALOGUE_NO, TEMP_CATALOGUENO_DESCRIPTION, TEMP_CATALOGUENO_DATABASE_NAME,
         TEMP_CATALOGUENO_CABLE_OBJECT_IDENTIFIER]]
    
    return \
        silver_06_s_cablecatalogue_number_generation_dataframe
