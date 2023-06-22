def convert_sigraph_to_new_name(
        column_name: str,
        sigraph_old_value: str,
        loop_picklist: dict,
        default_value: str = None) \
        -> str:
    if sigraph_old_value == 'null' or sigraph_old_value is None:
        return \
            default_value

    else:
        if (column_name.upper(), sigraph_old_value.upper()) in loop_picklist.keys():
            return \
                loop_picklist[(column_name.upper(), sigraph_old_value.upper())]

        else:
            return \
                default_value
