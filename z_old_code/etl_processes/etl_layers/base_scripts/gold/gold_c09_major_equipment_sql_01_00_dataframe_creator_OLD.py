import pandas


def create_dataframe_gold_c09_major_equipment_sql_01_00(
        database_names_dataframe: pandas.DataFrame,
        major_equipments_dataframe: pandas.DataFrame) \
        -> pandas.DataFrame:
    filtered_major_equipments = \
        major_equipments_dataframe[major_equipments_dataframe['database_name'].isin(
            database_names_dataframe['Database_name'])]

    column_mapping = \
        {
            'Area': 'Area',
            'Parent_Equipment_No': 'Parent_Equipment_No',
            'Description': 'Description',
            'EquipmentType': 'EquipmentType',
            'VendorSupplied': 'VendorSupplied',
            'DwgRequired': 'DwgRequired',
            'Status': 'Status',
            'AreaPath': 'AreaPath',
            'Type': 'Type',
            'Designation': 'Designation',
            'Comment': 'Comment',
            'Installation_site': 'Installation site',
            'Category': 'Category'
        }

    major_equipments_dataframe = \
        filtered_major_equipments[column_mapping.keys()].rename(
            columns=column_mapping)

    # Note: 'writeable' is not a valid flag - valid flags are: 'copy' and 'allows_duplicate_labels'
    # major_equipments_dataframe.set_flags(
    #     writeable=False)

    return \
        major_equipments_dataframe
