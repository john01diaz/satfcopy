
def custom_sort_pairs(items):
    pair_cables_list = []
    pair1,pair2,pair3,pair4,others = [],[],[],[],[] 
    priority_values = {
        'BU' : 1,
        'RD' : 1,
        'GY' : 2,
        'YE' : 2,
        'GN' : 3,
        'BN' : 3,
        'WH' : 4,
        'BK' : 4
    }
    for item in items:
        if item in priority_values.keys():
            if priority_values[item] == 1:
                pair1.append(item)
            elif priority_values[item] == 2:
                pair2.append(item)
            elif priority_values[item] == 3:
                pair3.append(item)
            elif priority_values[item] == 4:
                pair4.append(item)
        else:
            others.append(item)
    pair_cables_list.append(pair1)
    pair_cables_list.append(pair2)
    pair_cables_list.append(pair3)
    pair_cables_list.append(pair4)
    pair_cables_list.append(others)

    return pair_cables_list


def find_group_type(cable_colours):
    cable_colours = cable_colours.strip()
    if len(cable_colours) == 0:
        return "Cores" 
    colours = [item for item in cable_colours.split(",") if len(item.strip()) > 0]
    if 'SC' in colours:
        colours.remove('SC')
    if 'SL' in colours:
        colours.remove('SL')
    no_of_colours = len(colours)

    if no_of_colours%2 != 0 or no_of_colours == 0:
        return "Cores"

    else:
        sorted_pairs  = custom_sort_pairs(colours)
        if len(sorted_pairs[4]) >0 :
            return "Cores"
        elif all(len(pair) ==2 for pair in sorted_pairs[0:4] if len(pair)!=0):
            return "Pairs"
        else:
            return "Cores"

spark.udf.register("FindGroupType", find_group_type, StringType())