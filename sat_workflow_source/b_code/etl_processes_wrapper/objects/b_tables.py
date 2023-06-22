import pandas


class BTables:
    def __init__(
            self,
            table: pandas.DataFrame,
            identifier_column_names: list):
        self.table = \
            table

        self.identifier_column_names = \
            identifier_column_names
