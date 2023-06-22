import re
from pyspark.shell import spark


# @udf(returnType=ArrayType(StringType()))
def array_elements_sorting(column_list):
    column_list_1 = [i for i in column_list if i != ""]
    with_pin_group = []
    without_pin_group = []
    with_alpha_pins = []
    for i in column_list_1:
        if ":" in i:
            with_pin_group.append(i)
        elif i.isalpha():
            with_alpha_pins.append(i)
        else:
            without_pin_group.append(i)

    without_pin_group.sort(
        key=lambda x: int(re.search("(\d+)", x).group(1))
        if (re.search("(\d+)[A-Za-z+-]*", x))
        else 99
    )
    final_list = without_pin_group + sorted(with_alpha_pins) + sorted(with_pin_group)
    return final_list


spark.udf.register("array_elements_sorting", array_elements_sorting)
