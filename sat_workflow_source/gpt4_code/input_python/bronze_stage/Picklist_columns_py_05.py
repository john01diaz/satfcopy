
class_column_list = df.rdd.map(lambda x:(x[0],x[1])).collect()

