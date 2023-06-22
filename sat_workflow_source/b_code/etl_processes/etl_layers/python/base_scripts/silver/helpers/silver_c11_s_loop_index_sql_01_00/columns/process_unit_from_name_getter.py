def get_process_unit_from_name(
        name: str,
        cs_loop_unit: str) \
        -> str:
    first_name_component = \
        name.split(sep='_')[0]

    # When L.Name like 'RAUM%' Then Substring(Replace(L.name,'RAUM_',''),1,100)
    if first_name_component == 'RAUM':
        process_unit = \
            name.replace(
                'RAUM_',
                '')

    # When Substring(L.name, 1, charindex('_', L.Name) - 1) = CS_loop_unit Then Substring(L.name, charindex('_', L.Name) + 1, 100)
    elif first_name_component == cs_loop_unit:
        process_unit = \
            first_name_component + '_' + name.split(sep='_')[1][:1]

    # Else Substring(L.name, 1, charindex('_', L.Name) - 1)
    else:
        process_unit = \
            first_name_component

    return \
        process_unit
