def create_instrument_identification_view(
        loop_elements_dataframe,
        s_item_function_dataframe,
        device_type_dataframe):
    cte1 = _create_first_temp_dataframe(
            loop_elements_dataframe,
            s_item_function_dataframe)
    cte2 = _create_second_temp_dataframe(
            loop_elements_dataframe,
            cte1)
    cte3 = _add_device_type_conditions(
            cte2,
            device_type_dataframe)
    instrument_identification_view = pd.concat(
            [cte1, cte3],
            ignore_index=True)
    return instrument_identification_view


def _create_first_temp_dataframe(
        loop_elements_dataframe,
        s_item_function_dataframe):
    dataframe = loop_elements_dataframe.merge(
            s_item_function_dataframe,
            left_on=[Loop_elements_FilteredFor09SFieldDeviceCatalogueSql0100Sql.LOOP_ELEMENT_DATABASE_NAME.value,
                     Loop_elements_FilteredFor09SFieldDeviceCatalogueSql0100Sql.LOOP_ELEMENT_DYNAMIC_CLASS.value,
                     Loop_elements_FilteredFor09SFieldDeviceCatalogueSql0100Sql.LOOP_ELEMENT_OBJECT_IDENTIFIER.value],
            right_on=[S_ItemFunction.DATABASE_NAME.value,
                      S_ItemFunction.DYNAMIC_CLASS.value,
                      S_ItemFunction.TYPE.value],
            how='inner'
            )
    dataframe = dataframe[dataframe[
                              Loop_elements_FilteredFor09SFieldDeviceCatalogueSql0100Sql
                              .CS_LOOP_CS_LOOP_ELEMENT_DYN_CLASS.value] == 'CS_Loop_spez']
    dataframe = dataframe[dataframe[S_ItemFunction.TYPE.value] == 'Field Device']
    dataframe = dataframe[dataframe[
        Loop_elements_FilteredFor09SFieldDeviceCatalogueSql0100Sql.LC_ITEM_FUNCTION_CS_LOOP_ELEMENT_DYN_CLASS.value
    ].notnull()]
    dataframe['Type'] = 1
    return dataframe


def _create_second_temp_dataframe(
        loop_elements_dataframe,
        cte1):
    dataframe = loop_elements_dataframe.merge(
            cte1,
            left_on=[Loop_elements_FilteredFor09SFieldDeviceCatalogueSql0100Sql.LOOP_ELEMENT_DATABASE_NAME.value,
                     Loop_elements_FilteredFor09SFieldDeviceCatalogueSql0100Sql.LOOP_ELEMENT_DYNAMIC_CLASS.value,
                     Loop_elements_FilteredFor09SFieldDeviceCatalogueSql0100Sql.LOOP_ELEMENT_OBJECT_IDENTIFIER.value],
            right_on=[S_Field_Device_Catalogue.DATABASE_NAME.value,
                      S_Field_Device_Catalogue.DYNAMIC_CLASS.value,
                      S_Field_Device_Catalogue.OBJECT_IDENTIFIER.value],
            how='left',
            indicator=True
            )
    
    dataframe = dataframe[dataframe['_merge'] == 'left_only']
    dataframe = dataframe[dataframe[
                              Loop_elements_FilteredFor09SFieldDeviceCatalogueSql0100Sql
                              .CS_LOOP_CS_LOOP_ELEMENT_DYN_CLASS.value] == 'CS_Loop_spez']
    dataframe = dataframe[dataframe[
        Loop_elements_FilteredFor09SFieldDeviceCatalogueSql0100Sql.LC_ITEM_FUNCTION_CS_LOOP_ELEMENT_DYN_CLASS.value
    ].isnull()]
    
    condition = (dataframe[
                     Loop_elements_FilteredFor09SFieldDeviceCatalogueSql0100Sql.CS_LOCATION_FULL_DESIGNATION.value
                 ].isnull()) | (
                    dataframe[
                        Loop_elements_FilteredFor09SFieldDeviceCatalogueSql0100Sql.CS_DEVICE_TYPE.value].str.upper(
                
                            ).str.contains(
                            'INDUCTIVE SENSOR'))
    
    dataframe = dataframe[condition]
    
    dynamic_class_exclusions = ['CS_Loop_element_hw_bi', 'CS_Loop_element_hw_bo', 'CS_Loop_element_hw_ai',
                                'CS_Loop_element_hw_ao']
    dataframe = dataframe[
        ~dataframe[Loop_elements_FilteredFor09SFieldDeviceCatalogueSql0100Sql.LOOP_ELEMENT_DYNAMIC_CLASS.value].isin(
                dynamic_class_exclusions)]
    return dataframe


def _add_device_type_conditions(
        cte2,
        device_type_dataframe):
    dataframe = cte2.merge(
            device_type_dataframe,
            left_on=Loop_elements_FilteredFor09SFieldDeviceCatalogueSql0100Sql.CS_DEVICE_TYPE.value,
            right_on=DeviceType.SIGRAPHDEVICETYPE.value,
            how='left'
            )
    
    dataframe[DeviceType.REVISEDDEVICETYPE.value] = dataframe[DeviceType.REVISEDDEVICETYPE.value].replace(
            ['ANALOG INPUT', 'ANALOG OUTPUT', 'DIGITAL INPUT', 'DIGITAL OUTPUT'],
            ['AI', 'AO', 'DI', 'DO'])
    dataframe[DeviceType.REVISEDDEVICETYPE.value] = dataframe[DeviceType.REVISEDDEVICETYPE.value].fillna(
            dataframe[Loop_elements_FilteredFor09SFieldDeviceCatalogueSql0100Sql.CS_DEVICE_TYPE.value])
    
    device_types_exclusions = ['C300 DO', 'DCS AI', 'FTA DO', 'IOTA AI', 'PLC DO', 'PLS AO', 'PLS DI', 'PLS DO',
                               'PLS OUTBOUND', 'SM AI', 'SM DI', 'SM DO', 'SPS AI', 'SPS DI']
    dataframe = dataframe[~dataframe[DeviceType.REVISEDDEVICETYPE.value].isin(
            device_types_exclusions)]
    dataframe['Type'] = 2
    return dataframe
