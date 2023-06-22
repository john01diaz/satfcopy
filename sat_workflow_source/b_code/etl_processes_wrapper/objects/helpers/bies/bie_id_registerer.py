from nf_common_source.code.services.b_dictionary_service.objects.row_b_dictionaries import RowBDictionaries
from nf_common_source.code.services.identification_services.b_identity_ecosystem.objects.bie_ids import BieIds
from sat_workflow_source.b_code.etl_processes_wrapper.objects.bie_sub_registers import BieSubRegisters


def register_bie_id(
        bie_sub_register: BieSubRegisters,
        bie_id: BieIds,
        bie_id_type: str,
        name: str = str(),
        parent_bie_id: BieIds = None) \
        -> None:
    row_dictionary = \
        {
            'bie_ids': bie_id,
            'bie_id_types': bie_id_type,
            'names': name,
            'parent_bie_ids': parent_bie_id
        }

    row_b_dictionary = \
        RowBDictionaries(
            dictionary=row_dictionary)

    bie_sub_register.bie_id_register_table_b_dictionary.add_new_row_b_dictionary(
        bie_row_id=bie_sub_register.bie_id_register_table_b_dictionary.get_next_bie_row_id(),
        row_b_dictionary=row_b_dictionary)
