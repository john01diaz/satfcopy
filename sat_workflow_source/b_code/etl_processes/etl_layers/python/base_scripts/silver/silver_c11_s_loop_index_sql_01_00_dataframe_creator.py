import pandas
import re

# Define constants
from sat_workflow_source.b_code.etl_schemas.bronze_stage.filtered.loop_filtered_for_11_S_Loop_Index_sql_01_00_sql import \
    LoopFilteredFor11SLoopIndexSql0100Sql
from sat_workflow_source.b_code.etl_schemas.bronze_stage.layer import Layer
from sat_workflow_source.b_code.etl_schemas.crosswalks.pbs_table import PBS_Table
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_loop_index import S_LoopIndex

TEMPLATE_LOOP_FALSE = "FALSE"


# Load input dataframes as parameters
def create_silver_c11_s_loop_index_sql_01_00_dataframe(
        input_tables: dict):
    layer_dataframe = \
        input_tables['Layer']

    loop_dataframe = \
        input_tables['Loop']

    pbs_dataframe = \
        input_tables['PBS_Table']

    # Preprocessing for join
    layer_dataframe = layer_dataframe[[Layer.DATABASE_NAME.value, Layer.OBJECT_IDENTIFIER.value, Layer.DYNAMIC_CLASS.value]]
    loop_dataframe = loop_dataframe[[LoopFilteredFor11SLoopIndexSql0100Sql.LOOP_DATABASE_NAME.value, LoopFilteredFor11SLoopIndexSql0100Sql.LAYER_CS_LOOP_HREF.value,
                                     LoopFilteredFor11SLoopIndexSql0100Sql.LAYER_CS_LOOP_DYN_CLASS.value]]
    pbs_dataframe = pbs_dataframe[[PBS_Table.PROCESS_UNIT.value, PBS_Table.AREA_CODE.value]]
    
    # Inner joins
    df_joined = pandas.merge(
        layer_dataframe,
        loop_dataframe,
        left_on=[Layer.DATABASE_NAME.value, Layer.OBJECT_IDENTIFIER.value, Layer.DYNAMIC_CLASS.value],
        right_on=[LoopFilteredFor11SLoopIndexSql0100Sql.LOOP_DATABASE_NAME.value, LoopFilteredFor11SLoopIndexSql0100Sql.LAYER_CS_LOOP_HREF.value,
                  LoopFilteredFor11SLoopIndexSql0100Sql.LAYER_CS_LOOP_DYN_CLASS.value])
    
    # Use apply to replace the SQL CASE statement for the Process_Unit column before merging
    df_joined['Process_Unit'] = df_joined.apply(
        lambda
            row: extract_process_unit(
                row[Layer.NAME.value],
                row[Layer.CS_LOOP_UNIT.value]),
        axis=1)
    
    # Second join
    df_joined = pandas.merge(
        df_joined,
        pbs_dataframe,
        left_on=['Process_Unit', LoopFilteredFor11SLoopIndexSql0100Sql.CS_LOOP_ID.value],
        right_on=[PBS_Table.PROCESS_UNIT.value, PBS_Table.AREA_CODE.value])
    
    # Filter data
    df_joined = df_joined[df_joined[Layer.TEMPLATE_LOOP.value] == TEMPLATE_LOOP_FALSE]
    
    # Manipulate columns and create new ones. This part replaces the SELECT part of the SQL script.
    df_joined = manipulate_columns(
        df_joined)
    
    return df_joined


def extract_process_unit(
        name: str,
        cs_loop_unit: str):
    if name.startswith(
            'RAUM'):
        return name.replace(
            'RAUM_',
            '')[:100]
    elif name.split(
            '_')[0] == cs_loop_unit:
        return name.split(
            '_')[1][:100]
    else:
        return name.split(
            '_')[0]


def manipulate_columns(
        df: pandas.DataFrame):
    # ... continue for all other columns
    df[S_LoopIndex.LOOPNO.value] = df.apply(
        lambda
            row: extract_loop_no(
                row[LoopFilteredFor11SLoopIndexSql0100Sql.CS_LOOP_ID.value]),
        axis=1)
    df[S_LoopIndex.AREA.value] = df.apply(
        lambda
            row: extract_area(
                row[LoopFilteredFor11SLoopIndexSql0100Sql.CS_LOOP_ID.value]),
        axis=1)
    df[S_LoopIndex.NUMBER.value] = df.apply(
        lambda
            row: extract_number(
                row[LoopFilteredFor11SLoopIndexSql0100Sql.CS_LOOP_ID.value]),
        axis=1)
    # ...


def extract_loop_no(
        cs_loop_id: str):
    if '*' in cs_loop_id or '?' in cs_loop_id or '~' in cs_loop_id:
        return cs_loop_id.translate(
                {ord(
                    i): None for i in '*?~'})
    else:
        return cs_loop_id


def extract_area(
        cs_loop_id: str):
    return re.match(
        r'^[0-9]+',
        cs_loop_id)[0]


def extract_number(
        cs_loop_id: str):
    return re.match(
        r'^[0-9]+[A-Z]+([0-9]+)',
        cs_loop_id)[1]

