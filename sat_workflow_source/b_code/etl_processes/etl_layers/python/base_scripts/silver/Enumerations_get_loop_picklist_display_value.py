from importlib.resources import files
import pandas


def get_loop_picklist_display_value(
        enumeration):
    if enumeration[1] == 'null' or enumeration[1] == None:
        return \
            None

    else:
        enumeration[0] = \
            enumeration[0].upper()

        enumeration[1] = \
            enumeration[1].upper()

        if enumeration[0] + ":" + enumeration[1] in loop_picklist_enumerations.keys():
            return \
                loop_picklist_enumerations[enumeration[0] + ":" + enumeration[1]]

        else:
            return \
                None


resource_full_file_path = \
    files(
        'sat_workflow_source.c_resources').joinpath('Loop_List_Cross_walk.xlsx')

Looplist_picklist_crosswalk = \
    pandas.read_excel(
        str(resource_full_file_path),
        sheet_name='Sheet1',
        dtype=str,
        na_filter=False)

Looplist_picklist_crosswalk['Sigraph_Value_check'] = \
    (Looplist_picklist_crosswalk['Column_Name'].astype(str) + ':' + Looplist_picklist_crosswalk[
        'Sigraph_Old_Value'].astype(str)).str.upper()

loop_picklist_enumerations = \
    {}

for index, enumeration in Looplist_picklist_crosswalk.iterrows():
    loop_picklist_enumerations[enumeration['Sigraph_Value_check']] = \
        enumeration['New_value']
