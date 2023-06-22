
@udf(returnType=StringType())
def to_get_loop_format_tag(loop_id):
    loop_id = loop_id.strip()
    if re.search("^[0-9]+[A-Z]+[0-9]+[_][0-9A-Z]+", loop_id):
        return "RHLND Loop Tag 2"
    if re.search("^[0-9]+[A-Z]+[0-9]+[ ][0-9A-Z]+", loop_id):
        return "RHLND Loop Tag 2"
    elif re.search("^[0-9]+[A-Z]+[0-9]+[.][0-9A-Z]+", loop_id):
        return "RHLND Loop Tag 3"
    elif re.search("^[0-9]+[A-Z]+[0-9]+[/][0-9A-Z]+", loop_id):
        return "RHLND Loop Tag 4"
    elif re.search("^[A-Z]+[0-9]+[_][0-9A-Z]+", loop_id):
        return "RHLND Loop Tag 5"
    elif re.search("^[A-Z]+[_][0-9A-Z]+", loop_id):
        return "RHLND Loop Tag 7"
    elif re.search("^[A-Z]+[0-9]+[A-Z0-9]+", loop_id):
        return "RHLND Loop Tag 9"
    elif re.search("^[0-9]+[A-Z]+[0-9]+[-][0-9A-Z]+", loop_id):
        return "RHLND Loop Tag 6"
    elif re.search("^[A-Z]+[_][A-Z0-9]+[.][A-Z0-9]+", loop_id):
        return "RHLND Loop Tag 11"
    elif re.search("^[0-9A-Z]+$", loop_id):
        return "RHLND Loop Tag 1"
    else:
        return "NO TAG"


spark.udf.register("to_get_loop_format_tag", to_get_loop_format_tag)

