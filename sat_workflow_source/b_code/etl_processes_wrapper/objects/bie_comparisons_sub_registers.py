from nf_common_source.code.services.b_dictionary_service.objects.table_b_dictionaries import TableBDictionaries
from nf_common_source.code.services.identification_services.b_identity_ecosystem.bie_id_creators.bie_id_sum_from_strings_creator import \
    create_bie_id_sum_from_strings


class BieComparisonsSubRegisters:
    def __init__(
            self,
            owning_registry):
        self.owning_registry = \
            owning_registry

        self.comparison_tables = \
            dict()

        table_name = \
            'etl_analysis_bie_ids_comparison'

        bie_table_id = \
            create_bie_id_sum_from_strings(
                strings=['b_table', table_name])

        self.comparison_tables['etl_analysis_bie_ids_comparison'] = \
            TableBDictionaries(
                table_name=table_name,
                bie_table_id=bie_table_id)
