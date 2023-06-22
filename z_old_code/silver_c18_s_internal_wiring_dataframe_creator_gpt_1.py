import pandas

from sat_workflow_source.b_code.etl_schemas.silver_stage.s_connection import S_Connection
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_internal_wiring import S_Internal_Wiring
from sat_workflow_source.b_code.etl_schemas.silver_stage.s_terminals import S_Terminals


def create_silver_18_s_internal_wiring_sql_01_00_dataframe_gpt_1(
        s_connection_dataframe: pandas.DataFrame,
        s_terminals_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:

    # Assuming the input files are already loaded as pandas dataframes: s_connection_dataframe, s_terminals_dataframe

    # Perform the JOIN operations
    joined_df = s_connection_dataframe.merge(s_terminals_dataframe,
                                             left_on=[S_Connection.FROM_LOCATION.value, S_Connection.FROM_ITEM.value,
                                                      S_Connection.FROM_TERMINAL_MARKING.value],
                                             right_on=[S_Terminals.PARENT_EQUIPMENT_NO.value,
                                                       S_Terminals.EQUIPMENT_NO.value, S_Terminals.MARKING.value],
                                             how='inner') \
        .merge(s_terminals_dataframe, left_on=[S_Connection.TO_LOCATION.value, S_Connection.TO_ITEM.value,
                                               S_Connection.TO_TERMINAL_MARKING.value],
               right_on=[S_Terminals.PARENT_EQUIPMENT_NO.value, S_Terminals.EQUIPMENT_NO.value,
                         S_Terminals.MARKING.value], how='inner')

    # Apply column transformations and mappings
    internal_wiring_dataframe = pandas.DataFrame()
    internal_wiring_dataframe[S_Internal_Wiring.DATABASE_NAME.value] = joined_df[S_Connection.DATABASE_NAME.value]
    internal_wiring_dataframe[S_Internal_Wiring.OBJECT_IDENTIFIER.value] = joined_df[
        S_Connection.OBJECT_IDENTIFIER.value]
    internal_wiring_dataframe[S_Internal_Wiring.FROM_PARENT_EQUIPMENT_NO.value] = joined_df[
        S_Connection.FROM_LOCATION.value]
    internal_wiring_dataframe[S_Internal_Wiring.FROM_COMPARTMENT.value] = ''
    internal_wiring_dataframe[S_Internal_Wiring.FROM_EQUIPMENT.value] = joined_df[S_Connection.FROM_ITEM.value]
    internal_wiring_dataframe[S_Internal_Wiring.FROM_WIRE_LINK.value] = joined_df[S_Connection.CONNECTION_TYPE.value]
    internal_wiring_dataframe[S_Internal_Wiring.FROM_MARKING.value] = joined_df[
        S_Connection.FROM_TERMINAL_MARKING.value]

    # Perform conditional transformations
    internal_wiring_dataframe[S_Internal_Wiring.FROM_LEFT_RIGHT.value] = internal_wiring_dataframe.apply(
        lambda row: 'Normal' if (row[S_Connection.FROM_DYNAMIC_CLASS.value] == 'LC_Component_function'
                                 and row[S_Connection.FROM_TERMINAL_MARKING.value].upper().strip().split()[0] not in (
                                 'AB', 'CD', 'EF', 'GH'))
        else 'R', axis=1)

    internal_wiring_dataframe[S_Internal_Wiring.TO_PARENT_EQUIPMENT_NO.value] = joined_df[
        S_Connection.TO_LOCATION.value]
    internal_wiring_dataframe[S_Internal_Wiring.TO_COMPARTMENT.value] = ''
    internal_wiring_dataframe[S_Internal_Wiring.TO_EQUIPMENT_NO.value] = joined_df[S_Connection.TO_ITEM.value]
    internal_wiring_dataframe[S_Internal_Wiring.TO_WIRE_LINK.value] = joined_df[S_Connection.CONNECTION_TYPE.value]
    internal_wiring_dataframe[S_Internal_Wiring.TO_MARKING.value] = joined_df[S_Connection.TO_TERMINAL_MARKING.value]

    # Perform conditional transformations
    internal_wiring_dataframe[S_Internal_Wiring.TO_LEFT_RIGHT.value] = internal_wiring_dataframe.apply(
        lambda row: 'Normal' if (row[S_Connection.TO_DYNAMIC_CLASS.value] == 'LC_Component_function'
                                 and row[S_Connection.TO_TERMINAL_MARKING.value].upper().strip().split()[0] not in (
                                 'AB', 'CD', 'EF', 'GH'))
        else 'L', axis=1)

    internal_wiring_dataframe[S_Internal_Wiring.CLASS.value] = joined_df[S_Terminals.CLASS.value]

    # Filter rows based on conditions
    internal_wiring_dataframe = internal_wiring_dataframe[(internal_wiring_dataframe[
                                                               S_Internal_Wiring.FROM_PARENT_EQUIPMENT_NO.value] ==
                                                           internal_wiring_dataframe[
                                                               S_Internal_Wiring.TO_PARENT_EQUIPMENT_NO.value])
                                                          & internal_wiring_dataframe[
                                                              S_Internal_Wiring.FROM_PARENT_EQUIPMENT_NO.value].notnull()
                                                          & internal_wiring_dataframe[
                                                              S_Internal_Wiring.TO_PARENT_EQUIPMENT_NO.value].notnull()]

    return \
        internal_wiring_dataframe
