import pandas

from sat_workflow_source.b_code.b_identity_ecosystem.bie_id_creators.bie_id_sum_from_bie_ids_creator import \
    create_bie_id_sum_from_bie_ids
from sat_workflow_source.b_code.b_identity_ecosystem.objects.bie_ids import BieIds


table = \
    pandas.DataFrame(
        columns=['bie_ids', 'name'])

for row_number in range(10):
    bie_id = BieIds(int_value=row_number)

    table.loc[row_number] = \
        [
            bie_id,
            str(row_number)
        ]

print(table)

for bie_id in table['bie_ids'].values:
    print(
        bie_id.int_value)
    
bie_id = \
    create_bie_id_sum_from_bie_ids(
        bie_ids=table['bie_ids'].values)

print(bie_id)

print(bie_id.int_value)
