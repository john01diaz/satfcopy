

def replace_qualify_string_to_having(
        command_string: str) \
        -> str:
    if 'QUALIFY' in command_string.upper():
        command_string = \
            command_string.replace(
                'Qualify', 'HAVING').replace(
                'qualify', 'HAVING').replace(
                'QUALIFY', 'HAVING')

    return \
        command_string
