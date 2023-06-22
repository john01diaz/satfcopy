import pandas


def create_silver_08_s_device_catalogue_dataframe(
        s_itemfunction_dataframe: pandas.DataFrame,
        s_item_function_model_dataframe: pandas.DataFrame):
    # silver_08_s_device_catalogue_dataframe = \
    #     pandas.DataFrame(columns=['A', 'B', 'C']); #TODO Add code here


    # def create_vw_devicecatalogue_extract(
    #         s_itemfunction_dataframe,
    #         s_item_function_model_dataframe):
    #     # Perform the inner join operation
        silver_08_s_device_catalogue_dataframe = pandas.merge(
            s_itemfunction_dataframe,
            s_item_function_model_dataframe,
            left_on=['database_name', 'Item_Dynamic_Class', 'Item_Object_identifier', 'Dynamic_Class',
                     'Object_Identifier'],
            right_on=['database_name', 'Item_Dynamic_Class', 'Item_Object_identifier', 'Dynamic_Class',
                      'Object_Identifier'],
            how='inner')

        # Filter rows based on conditions
        silver_08_s_device_catalogue_dataframe = silver_08_s_device_catalogue_dataframe[(silver_08_s_device_catalogue_dataframe['Type'].isin(
            ['Device', 'FTA'])) &
                              (silver_08_s_device_catalogue_dataframe['location_designation'].notnull()) &
                              ((silver_08_s_device_catalogue_dataframe['Left'].fillna(
                                  0) + silver_08_s_device_catalogue_dataframe['Right'].fillna(
                                  0)) > 0)]

        # Create 'Description' column
        silver_08_s_device_catalogue_dataframe['Description'] = silver_08_s_device_catalogue_dataframe.apply(
            lambda
                row: 'RHLDD' if pandas.isna(
                row['Description']) and pandas.isna(
                row['Product_Key']) and pandas.isna(
                row['Document_Number'])
            else f"{row['Description']}-{row['Product_Key']}-{row['Document_Number']}-"
                 f"{row['Database_name'].replace('_2016R3', '')}",
            axis=1)

        # Add constant 'AllowUse' column
        silver_08_s_device_catalogue_dataframe['AllowUse'] = 'TRUE'

        # Rename 'Device_Type' column to 'Type'
        silver_08_s_device_catalogue_dataframe.rename(
            columns={
                'Device_Type': 'Type'
                },
            inplace=True)

        # Generate the 'Catalogue_RNT' column using 'rank' operation over 'ModelNo' and 'Item_Object_Identifier'
        silver_08_s_device_catalogue_dataframe['Catalogue_RNT'] = silver_08_s_device_catalogue_dataframe.sort_values(
            ['ModelNo', 'Item_Object_Identifier']).groupby(
            'ModelNo').cumcount() + 1

        # Remove duplicate rows
        silver_08_s_device_catalogue_dataframe.drop_duplicates(
            keep='first',
            inplace=True)

        return \
        silver_08_s_device_catalogue_dataframe