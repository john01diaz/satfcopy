
def multiple_replace(column, str):
    dict = json.loads(str)
    if column:
        for old_string in dict.keys():
            column = column.upper().replace(old_string, dict[old_string])
    return column


spark.udf.register("multipleReplace", multiple_replace, StringType())

