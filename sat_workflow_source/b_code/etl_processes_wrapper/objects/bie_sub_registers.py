from nf_common_source.code.services.b_dictionary_service.objects.table_b_dictionaries import TableBDictionaries
from nf_common_source.code.services.identification_services.b_identity_ecosystem.bie_id_creators.bie_id_sum_from_strings_creator import \
    create_bie_id_sum_from_strings


class BieSubRegisters:
    def __init__(
            self,
            owning_register):
        self.owning_register = \
            owning_register

        self.bie_tables = \
            dict()

        table_name = \
            'bie_id_register'

        bie_table_id = \
            create_bie_id_sum_from_strings(
                strings=['b_table', table_name])

        self.bie_id_register_table_b_dictionary = \
            TableBDictionaries(
                table_name=table_name,
                bie_table_id=bie_table_id)
