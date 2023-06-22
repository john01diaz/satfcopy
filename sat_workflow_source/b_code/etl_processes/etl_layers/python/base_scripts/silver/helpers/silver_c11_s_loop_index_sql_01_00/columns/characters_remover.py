def remove_characters(
        input_string: str,
        characters_to_remove: str) \
        -> str:
    if not input_string:
        return \
            str()

    translation_table = \
        str.maketrans(
            str(),
            str(),
            characters_to_remove)

    return \
        input_string.translate(
            translation_table)
