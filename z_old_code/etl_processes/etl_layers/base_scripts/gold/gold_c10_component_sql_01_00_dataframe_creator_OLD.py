import pandas


def create_dataframe_gold_c10_component_sql_01_00(
        s_component_dataframe: pandas.DataFrame,
        vw_database_names_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    # Note: added by hand to filter the set of columns
    columns_to_keep = \
        [
            'Parent_Equipment_No',
            'Equipment_No',
            'EquipmentType',
            'CatalogueNo',
            'DinRail',
            'Sequence',
            'Remarks'
        ]

    filtered_dataframe = \
        __filter_class_and_database_name(
            s_component_dataframe,
            vw_database_names_dataframe)

    component_dataframe = \
        __add_row_number(
            filtered_dataframe,
            columns_to_keep)

    return \
        component_dataframe


def __filter_class_and_database_name(
        s_component_dataframe: pandas.DataFrame,
        vw_database_names_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    columns_to_keep = \
        [
            'Parent_Equipment_No',
            'Equipment_No',
            'EquipmentType',
            'CatalogueNo',
            'DinRail',
            'Remarks',
            'Item_Object_identifier'
        ]

    filtered_df = \
        s_component_dataframe[
            (s_component_dataframe['Class'].isin(['Instrumentation', 'Inst(Shared)', 'Elec(Shared)']))
            & (s_component_dataframe['database_name'].isin(
                vw_database_names_dataframe['Database_name']))].copy()

    # Note: method edited by hand because GPT-4 prompt was incomplete
    filtered_df = \
        filtered_df.filter(
            items=columns_to_keep)

    return \
        filtered_df.drop_duplicates()


def __add_row_number(
        filtered_dataframe: pandas.DataFrame,
        columns_to_keep: list) \
        -> pandas.DataFrame:
    # Note: method edited by hand because GPT-4 prompt was incomplete
    filtered_dataframe['Sequence'] = \
        filtered_dataframe.groupby(
            ['Parent_Equipment_No', 'DinRail'])['Item_Object_identifier'].rank(method='first')

    # Note: previous prompt was wrong - updated by hand
    filtered_dataframe = \
        filtered_dataframe.sort_values(
            ['Parent_Equipment_No', 'DinRail', 'Item_Object_identifier'])[columns_to_keep]

    return \
        filtered_dataframe
