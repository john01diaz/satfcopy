import numpy


def add_bie_component(
        bie_components: list,
        component: str) \
        -> None:
    if component is None:
        component = \
            str()

    if component is numpy.nan:
        component = \
            str()

    else:
        component = \
            str(
                component)

    bie_components.append(
        component)
