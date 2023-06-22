import pandas

from z_sandpit.common.hardcoded_configurations import HardcodedConfigurations


def get_loop_picklist_from_model() \
        -> dict:
    # TODO - to be retrieved from model
    loop_picklist_dataframe = \
        pandas.read_excel(
            HardcodedConfigurations.loop_picklist_file_path)

    loop_picklist_dataframe = \
        loop_picklist_dataframe.fillna(str())

    loop_picklist = \
        dict()

    for index, loop_picklist_row in loop_picklist_dataframe.iterrows():
        __add_picklist_row_to_dictionary(
            loop_picklist=loop_picklist,
            loop_picklist_row=loop_picklist_row)

    return \
        loop_picklist


def __add_picklist_row_to_dictionary(
        loop_picklist: dict,
        loop_picklist_row) \
        -> None:
    column_name = \
        str(loop_picklist_row['Column_Name'])

    sigraph_old_value = \
        str(loop_picklist_row['Sigraph_Old_Value'])

    new_value = \
        str(loop_picklist_row['New_value'])

    key = \
        (
            column_name.upper(),
            sigraph_old_value.upper()
        )

    loop_picklist[key] = \
        new_value
