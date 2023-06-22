from nf_common_source.code.services.identification_services.b_identity_ecosystem.objects.bie_ids import BieIds
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from pandas import DataFrame


def get_table_name_from_bie_id(
        bie_id: BieIds,
        bie_id_register: DataFrame) \
        -> str:
    if not isinstance(bie_id, BieIds):
        # TODO: Covert this into a proper warning
        log_message(
            message='bie_id {0} is not a BieIds, but a {1}. When cast to a string it gives a value of: {2}'.format(
                bie_id,
                type(bie_id),
                str(bie_id)))

    bie_id_type = \
        list(bie_id_register[bie_id_register['bie_ids'] == bie_id]['bie_id_types'])[0]

    if bie_id_type == 'bie_table_ids':
        return \
            list(bie_id_register[bie_id_register['bie_ids'] == bie_id]['names'])[0]

    parent_bie_id = \
        list(bie_id_register[bie_id_register['bie_ids'] == bie_id]['parent_bie_ids'])[0]

    ancestor_table_name = \
        get_table_name_from_bie_id(
            bie_id=parent_bie_id,
            bie_id_register=bie_id_register)

    return \
        ancestor_table_name
