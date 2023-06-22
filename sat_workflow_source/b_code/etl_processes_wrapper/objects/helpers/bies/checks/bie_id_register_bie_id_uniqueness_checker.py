from nf_common_source.code.services.b_dictionary_service.objects.table_b_dictionaries import TableBDictionaries
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message


def check_bie_id_register_bie_id_uniqueness(
        bie_id_register: TableBDictionaries,
        bie_id_register_alias: str) \
        -> None:
    bie_id_register_bie_id_counts = \
        bie_id_register.pivot_table(index=['bie_ids'], aggfunc='size')

    duplicated_bie_ids = \
        bie_id_register_bie_id_counts[bie_id_register_bie_id_counts > 1]

    if len(duplicated_bie_ids) > 0:
        # TODO: log these as proper warnings
        log_message(
            message='WARNING - there are {0} duplicated bie_ids in the {1} register \n Which are: {2}'.format(
                len(duplicated_bie_ids),
                bie_id_register_alias,
                duplicated_bie_ids))

        bie_id_register_duplicated_rows = \
            bie_id_register[bie_id_register.duplicated()]

        log_message(
            message='WARNING - From these duplicated bie_ids, the following are full row duplications:\n{0}'.format(
                bie_id_register_duplicated_rows))
