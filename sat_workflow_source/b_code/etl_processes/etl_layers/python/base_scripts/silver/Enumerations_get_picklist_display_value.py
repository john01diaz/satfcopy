from importlib.resources import files
import pandas


def get_picklist_display_value(
        enumeration):
    if enumeration[1] == 'null' or enumeration[1] == None:
        return \
            None

    else:
        if enumeration[0] + ":" + enumeration[1] in picklist_enumerations.keys():
            return \
                picklist_enumerations[enumeration[0] + ":" + enumeration[1]]

        else:
            return \
                enumeration[1]


resource_full_file_path = \
    files(
        'sat_workflow_source.c_resources').joinpath('Picklist_ColumnName_Values.xlsx')

sigraph_picklist_crosswalk = \
    pandas.read_excel(
        str(resource_full_file_path),
        sheet_name='Sheet1',
        dtype=str,
        na_filter=False)

sigraph_picklist_crosswalk['Sigraph_Value_check'] = \
    (sigraph_picklist_crosswalk['PickListName'].astype(str) + ':' + sigraph_picklist_crosswalk['Value'].astype(
        str)).str.upper()

picklist_enumerations = \
    {}

for index, enumeration in sigraph_picklist_crosswalk.iterrows():
    picklist_enumerations[enumeration['Sigraph_Value_check']] = \
        enumeration['Value']
