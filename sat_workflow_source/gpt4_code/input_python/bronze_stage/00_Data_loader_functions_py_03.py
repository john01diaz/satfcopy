
schema = ArrayType(StructType([ 
    StructField("_dyn_class",StringType(),True), 
    StructField("_href",StringType(),True), 
    StructField("_index",StringType(),True), 
    StructField("_stat_type", StringType(), True)
]))

static_schema = ArrayType(StructType([ 
    StructField("_href",StringType(),True), 
    StructField("_index",StringType(),True), 
    StructField("_stat_type", StringType(), True)
]))

