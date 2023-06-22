def get_instrumentation_or_electrical(
        loop_dynamic_class: str) \
        -> str:
    if loop_dynamic_class == 'CS_Loop_spez':
        return \
            'Instrumentation'

    return \
        'Electrical'
