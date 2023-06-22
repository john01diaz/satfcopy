import pandas as pd

def filter_dataframe_by_columns(dataframe, column_names):
    return dataframe[column_names]

def map_dataframe_columns(dataframe, column_mappings):
    return dataframe.rename(columns=column_mappings)

def add_constant_columns(dataframe, constant_columns):
    for column_name, constant_value in constant_columns.items():
        dataframe[column_name] = constant_value
    return dataframe

def loop_index(dataframe_loop_index, dataframe_database_names, column_filters, column_mappings, constant_columns):
    filtered_dataframe = filter_dataframe_by_columns(dataframe_loop_index, column_filters)
    
    mapped_dataframe = map_dataframe_columns(filtered_dataframe, column_mappings)
    
    constant_added_dataframe = add_constant_columns(mapped_dataframe, constant_columns)
    
    filtered_class_dataframe = constant_added_dataframe[constant_added_dataframe['Class'] == 'Instrumentation']
    
    database_names = dataframe_database_names['Database_name'].unique()
    
    final_dataframe = filtered_class_dataframe[filtered_class_dataframe['database_name'].isin(database_names)].drop_duplicates()
    
    return final_dataframe


column_filters = ['Area', 'LoopNo', 'Function', 'Number', 'Suffix', 'Loop_Service_1', 'Status', 'Remarks', 
                  'FormatName', 'AreaPath', 'Loop_Function', 'Loop_Service_2', 'Loop_Service_3', 'SIFpro_Relevant', 
                  'Func_Description', 'Resp_Work_Center', 'Air_Distributor', 'Var_Part_of_Drawing_No', 
                  'Classification_by', 'Suppl_char_1', 'Visual_Inspection', 'EP_Origin', 'Y_Coord.', 'ET_structure', 
                  'Classification', 'Related_Report', 'X_Coord.', 'Field_Distrib_Output', 'Root_Extraction_Point', 
                  'Suppl_char_3', 'Suppl_char_2', 'Planning_Group', 'Tested_by', 'Test_Acc_to_Test_Catalog', 
                  'Test_Acc_by_Test_Catalog', 'Logic_Diag_Typical', 'Purpose_of_Inspection', 'Inspection_Interval', 
                  'Special_Req_3', 'Unit', 'Accomp_Documents', 'Special_Req_2', 'Field_Distrib_Input', 'Special_Req_1', 
                  'Realization_Pos', 'Function_Test', 'Graphical_Typical', 'Standard_Loop_ID', 'Status_Remark']

# column_mappings = {'LoopNo': 'Loop No', 'Loop_Service_1': 'Loop Service-1', 'Loop_Function': 'Loop Function',
#                    'Loop_Service_2': 'Loop Service-2', 'Loop_Service_3': 'Loop Service-3',
#                    'SIFpro_Relevant': 'SIFpro Relevant', 'Func_Description': 'Func Description',
#                    'Resp_Work_Center': 'Resp Work Center', 'Air_Distributor': 'Air Distributor',
#                    'Var_Part_of_Drawing_No': '



def add_constant_columns(dataframe, constant_columns):
    for column_name, constant_value in constant_columns.items():
        dataframe[column_name] = constant_value
    return dataframe


constant_columns = {'Wired': 'TRUE', 'Drawing': 'TRUE', 'ClassName': 'ILP'}

# rest of your code

# result_dataframe = loop_index(dataframe_loop_index, dataframe_database_names, column_filters, column_mappings, constant_columns)


column_mappings = {
    'LoopNo': 'Loop No', 
    'Loop_Service_1': 'Loop Service-1', 
    'Loop_Function': 'Loop Function', 
    'Loop_Service_2': 'Loop Service-2', 
    'Loop_Service_3': 'Loop Service-3', 
    'SIFpro_Relevant': 'SIFpro Relevant', 
    'Func_Description': 'Func Description', 
    'Resp_Work_Center': 'Resp Work Center', 
    'Air_Distributor': 'Air Distributor', 
    'Var_Part_of_Drawing_No': 'Var Part of Drawing No', 
    'Classification_by': 'Classification by', 
    'Suppl_char_1': 'Suppl char 1', 
    'Visual_Inspection': 'Visual Inspection', 
    'EP_Origin': 'EP Origin', 
    'Y_Coord.': 'Y Coord.', 
    'ET_structure': 'ET structure', 
    'Classification': 'Classification', 
    'Related_Report': 'Related Report', 
    'X_Coord.': 'X Coord.', 
    'Field_Distrib_Output': 'Field Distrib Output', 
    'Root_Extraction_Point': 'Root Extraction Point', 
    'Suppl_char_3': 'Suppl char 3', 
    'Suppl_char_2': 'Suppl char 2', 
    'Planning_Group': 'Planning Group', 
    'Tested_by': 'Tested by', 
    'Test_Acc_to_Test_Catalog': 'Test Acc to Test Catalog', 
    'Test_Acc_by_Test_Catalog': 'Test Acc by Test Catalog', 
    'Logic_Diag_Typical': 'Logic Diag Typical', 
    'Purpose_of_Inspection': 'Purpose of Inspection', 
    'Inspection_Interval': 'Inspection Interval', 
    'Special_Req_3': 'Special Req 3', 
    'Unit': 'Unit', 
    'Accomp_Documents': 'Accomp Documents', 
    'Special_Req_2': 'Special Req 2', 
    'Field_Distrib_Input': 'Field Distrib Input', 
    'Special_Req_1': 'Special Req 1', 
    'Realization_Pos': 'Realization Pos', 
    'Function_Test': 'Function Test', 
    'Graphical_Typical': 'Graphical Typical', 
    'Standard_Loop_ID': 'Standard Loop ID', 
    'Status_Remark': 'Status Remark'
}


# result_dataframe = loop_index(dataframe_loop_index, dataframe_database_names, column_filters, column_mappings, constant_columns)
