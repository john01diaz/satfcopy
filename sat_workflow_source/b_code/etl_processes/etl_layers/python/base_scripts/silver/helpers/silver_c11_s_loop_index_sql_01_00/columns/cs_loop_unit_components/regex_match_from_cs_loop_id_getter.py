import re


def get_regex_match_from_input_string(
        input_string: str,
        regex: str,
        group: int = 0) \
        -> str:
    if input_string:
        matches = \
            re.search(
                regex,
                input_string)

        if matches:
            regex_match = \
                matches.group(
                    group)
        else:
            regex_match = \
                None

    else:
        regex_match = \
            None

    return \
        regex_match
