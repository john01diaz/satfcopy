from enum import auto
from enum import Enum


class UsageTableTypes(
        Enum):
    NOT_SET = \
        auto()

    INPUT = \
        'input'

    OUTPUT = \
        'output'

    COMPARE_INPUT = \
        'compare_input'
