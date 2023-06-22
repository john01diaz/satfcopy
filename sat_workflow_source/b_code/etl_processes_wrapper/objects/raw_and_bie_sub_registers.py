from sat_workflow_source.b_code.etl_processes_wrapper.objects.bie_sub_registers import BieSubRegisters


class RawAndBieSubRegisters:
    def __init__(
            self,
            owning_registry):
        self.owning_registry = \
            owning_registry

        self.raw_sub_register = \
            dict()

        self.sql_create_table_statements = \
            dict()

        self.python_schema_enums = \
            dict()

        self.process_table_usages = \
            list()

        self.source_bie_sub_register = \
            BieSubRegisters(
                owning_register=self)

        self.generated_bie_sub_register = \
            BieSubRegisters(
                owning_register=self)

        self.source_before_bie_sub_register = \
            BieSubRegisters(
                owning_register=self)

        self.source_after_bie_sub_register = \
            BieSubRegisters(
                owning_register=self)
