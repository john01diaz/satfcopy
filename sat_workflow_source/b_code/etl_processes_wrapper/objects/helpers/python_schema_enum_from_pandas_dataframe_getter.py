from string import digits

from pandas import DataFrame


def get_python_schema_enum_from_pandas_dataframe(
        dataframe: DataFrame,
        table_name: str) \
        -> str:
    column_names = \
        dataframe.columns.tolist()

    import_line = '''from enum import Enum


'''

    base_class_name = \
        table_name.lstrip(digits).lstrip('_')

    if '.' in base_class_name:
        class_name = \
            base_class_name.split('.')[1]

    else:
        class_name = \
            base_class_name

    class_header_line = \
        f'class {class_name}(\n\t\tEnum):\n'

    enum_item_lines = \
        [
            f'\t{column_name.upper()} = \'{column_name}\'\n'
            for column_name in column_names
        ]

    python_schema_enum_string = \
        import_line + class_header_line + ''.join(enum_item_lines)

    return \
        python_schema_enum_string
