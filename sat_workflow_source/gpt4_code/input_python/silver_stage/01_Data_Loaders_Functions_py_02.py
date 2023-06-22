
def format_tag(loop_element_id):
    if loop_element_id:
        loop_element_id = loop_element_id.strip()
    else:
        return None
    if re.search("(^[A-Z]+)", loop_element_id):
        return "RHLND Instrument Tag 9"
    elif loop_element_id.isalnum():
        return "RHLND Instrument Tag 2"
    elif "/" in loop_element_id:
        return "RHLND Instrument Tag 4"
    elif " " in loop_element_id:
        return "RHLND Instrument Tag 5"
    elif "." in loop_element_id and any(
        i not in loop_element_id
        for i in ["!", "@", "$", "^", "&", "-", ";", ":", "?", "#", "*", "/"]
    ):
        dot_index = loop_element_id.index(".")
        underscore_index = loop_element_id.index("_") if "_" in loop_element_id else 0
        if re.search("(^[0-9]+)([A-Z]+)([0-9]+)([A-Z])", loop_element_id):
            if (
                re.search("(^[0-9]+)([A-Z]+)([0-9]+)([A-Z])", loop_element_id)
                .groups()[3]
                .isalpha()
            ):
                return "RHLND Instrument Tag 2"
        if dot_index < underscore_index or underscore_index == 0:
            return "RHLND Instrument Tag 1"
        else:
            return "RHLND Instrument Tag 3"
    elif "-" in loop_element_id:
        if re.search("(^[0-9]+)([A-Z]+)([\-])", loop_element_id):
            if re.search("([0-9]+)([A-Z]+)([\-])", loop_element_id).groups()[2] == "-":
                return "RHLND Instrument Tag 8"
        elif re.search("(^[0-9]+)([A-Z]+)([0-9]+)([A-Z]+)", loop_element_id):
            if (
                re.search("(^[0-9]+)([A-Z]+)([0-9]+)([A-Z])", loop_element_id)
                .groups()[3]
                .isalpha()
            ):
                return "RHLND Instrument Tag 2"
        elif re.search("(^[0-9]+)([A-Z]+)([0-9]+)([\-])", loop_element_id):
            if (
                re.search("(^[0-9]+)([A-Z]+)([0-9]+)([\-])", loop_element_id).groups()[
                    3
                ]
                == "-"
            ):
                return "RHLND Instrument Tag 6"
    elif "_" in loop_element_id:
        return "RHLND Instrument Tag 3"
    else:
        return "RHLND Instrument Tag 2"

