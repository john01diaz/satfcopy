from sat_workflow_source.b_code.etl_processes.gold_layer.base_scripts.c08_Instrument_Index import \
    create_instrument_index_dataframe
from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_universes import \
    EtlProcessesWrapperUniverses


def run_c08_instrument_index(
        etl_processes_wrapper_universe: EtlProcessesWrapperUniverses) \
        -> None:
    s_instrument_index = \
        etl_processes_wrapper_universe.get_and_index_input_dataframe(
            table_name='S_Instrument_Index',
            table_type='input').table

    c08_instrument_index_dataframe = \
        create_instrument_index_dataframe(
            s_instrument_index=s_instrument_index)

    etl_processes_wrapper_universe.register_generated_and_index_output_table(
        table=c08_instrument_index_dataframe,
        table_name='Instrument_Index',
        table_type='new_output',
        identifier_column_names=[])
