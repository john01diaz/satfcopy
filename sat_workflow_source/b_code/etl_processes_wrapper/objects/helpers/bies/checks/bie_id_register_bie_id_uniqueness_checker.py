from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from pandas import DataFrame


def check_bie_table_bie_ids_for_uniqueness(
        table_name: str,
        bie_table: DataFrame) \
        -> None:
    bie_ids = \
        bie_table['bie_ids'].tolist()

    distinct_bie_ids = \
        dict()

    for bie_id in bie_ids:
        if bie_id not in distinct_bie_ids.keys():
            distinct_bie_ids[bie_id] = \
                1

        else:
            distinct_bie_ids[bie_id] += \
                1

    table_has_unique_bie_ids = \
        True

    for bie_id in distinct_bie_ids.keys():
        if distinct_bie_ids[bie_id] > 1:
            table_has_unique_bie_ids = \
                False

            log_message(
                "ERROR: Duplicate bie_id " + str(bie_id) + " in table " + table_name + " with count " + str(distinct_bie_ids[bie_id]))

    if table_has_unique_bie_ids:
        log_message(
            'All bie_ids in table ' + table_name + ' are unique')
