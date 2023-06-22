from enum import Enum


class OriginTableTypes(
        Enum):
    NOT_SET = \
        'not_set'

    SOURCE = \
        'source'

    GENERATED = \
        'generated'

    SOURCE_AFTER = \
        'source_after'

    SOURCE_BEFORE = \
        'source_before'
