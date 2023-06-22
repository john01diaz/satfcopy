
df = sigraph_dyn_class_dict['Component-comp_measure_unit_qualifier']
for key in sigraph_dyn_class_dict:
    applogger.info(f"{key}")
    df = sigraph_dyn_class_dict[key].union(df)

