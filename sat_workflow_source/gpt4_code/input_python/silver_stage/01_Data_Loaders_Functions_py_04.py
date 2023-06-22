
def loop_element_id_suffix(loop_element_id, area, function, sequence_num):
    if loop_element_id is None:
        return None
    area = "" if area is None else area
    function = "" if function is None else function
    sequence_num = "" if sequence_num is None else sequence_num
    id = area + function + sequence_num
    len_id = len(id)
    suffix_index = loop_element_id.find(id)
    suffix_starts = suffix_index + len_id
    if suffix_index != -1:
        suffix = loop_element_id[suffix_starts:]
        suffix = (
            suffix[1:]
            if suffix.startswith(".")
            or suffix.startswith("/")
            or suffix.startswith("_")
            or suffix.startswith("-")
            or suffix.startswith("*")
            or suffix.startswith(" ")
            else suffix
        )
        suffix = (
            suffix[1:]
            if suffix.startswith(".")
            or suffix.startswith("/")
            or suffix.startswith("_")
            or suffix.startswith("-")
            else suffix
        )
        suffix = suffix[0 : len(suffix) - 1] if suffix.endswith(".") else suffix
        return suffix
    else:
        return "Not_found"

