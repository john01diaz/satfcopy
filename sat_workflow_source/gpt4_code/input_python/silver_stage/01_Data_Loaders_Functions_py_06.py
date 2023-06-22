
@udf(returnType=StringType())
def get_process_unit(layer_Pv_base_element, layer_CS_loop_unit):
    if layer_Pv_base_element == None:
        return None
    list = ["[", "(", ")", "]"]
    if any(layer_CS_loop_unit for i in list):
        if re.findall("\[.*?\]", layer_CS_loop_unit):
            element = re.findall("\[.*?\]", layer_CS_loop_unit)[0]
            layer_CS_loop_unit = element[1 : len(element) - 1]

    if any(i in layer_Pv_base_element for i in list):
        base_element = layer_Pv_base_element
        base_element = re.sub(r"[\([)\]]", "", base_element).strip()
        base_element = re.sub(r" ", "_", base_element)
        base_element = re.sub(r"-", "", base_element)
    else:
        if layer_CS_loop_unit is None:
            base_element = layer_Pv_base_element.strip()
            base_element = re.sub(r" ", "_", base_element)
            base_element = re.sub(r"-", "", base_element)
        else:
            base_element = layer_CS_loop_unit + "_" + layer_Pv_base_element
            base_element = re.sub(r" ", "_", base_element)
            base_element = re.sub(r"-", "", base_element)
    base_element = (
        base_element[0 : len(base_element) - 1]
        if base_element.endswith("_")
        else base_element
    )
    return base_element


@udf(returnType=StringType())
def lno_column(loopid, looppv):
    if looppv is None:
        return None

    sequence_no = loopid.split(looppv)[1]
    if sequence_no.isdigit():
        return sequence_no
    else:
        sequence_no = re.findall(r"[\d]*", sequence_no)[0]
        return sequence_no


@udf(returnType=StringType())
def loopsuffix_column(loopid, sequ):
    if sequ is None or len(sequ) == 0:
        return None
    else:
        suffix = loopid.split(sequ)[1]
        suffix = (
            suffix[1:]
            if suffix.startswith("_")
            or suffix.startswith(".")
            or suffix.startswith("/")
            else suffix
        )
        return suffix


@udf(returnType=StringType())
def loopsuffix_column_2(loop_id, LoopPV):
    if LoopPV is None or len(LoopPV) == 0:
        return None
    index = loop_id.find(LoopPV) + len(LoopPV)
    suffix = loop_id[index:]
    suffix = (
        suffix[1:]
        if suffix.startswith("_") or suffix.startswith(".") or suffix.startswith("/")
        else suffix
    )
    return suffix


spark.udf.register("get_process_unit", get_process_unit)
spark.udf.register("get_enumeration_display_value", get_enumeration_display_value)

