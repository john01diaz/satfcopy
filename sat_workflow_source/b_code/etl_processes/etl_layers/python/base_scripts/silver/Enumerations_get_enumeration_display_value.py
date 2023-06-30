from importlib.resources import files
import pandas


def get_enumeration_display_value(
        enumeration):
    if enumeration[1] == None:
        return \
            None

    if enumeration[0] == None:
        return \
            enumeration[1]

    else:
        if enumeration[0] + ":" + enumeration[1] in enumerations.keys():
            return \
                enumerations[enumeration[0] + ":" + enumeration[1]]

        else:
            return \
                enumeration[1]


resource_full_file_path = \
    files(
        'sat_workflow_source.c_resources').joinpath('sigraph_dyn_enums.txt')

sigraph_enumerations = \
    pandas.read_csv(
        str(resource_full_file_path),
        sep=",",
        quotechar='"',
        header=None)

enumerations = \
    {}

for index, enumeration in sigraph_enumerations.iterrows():
    enumerations[enumeration[1]] = \
        enumeration[2]
