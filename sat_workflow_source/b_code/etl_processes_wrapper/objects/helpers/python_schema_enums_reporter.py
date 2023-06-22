import os.path
from nf_common_source.code.services.reporting_service.reporters.log_file import LogFiles
from sat_workflow_source.b_code.etl_processes_wrapper.objects.raw_and_bie_sub_registers import RawAndBieSubRegisters


def report_python_schema_enums(
        etl_processes_wrapper_registry) \
        -> None:
    from sat_workflow_source.b_code.etl_processes_wrapper.objects.etl_processes_wrapper_registries import \
        EtlProcessesWrapperRegistries

    if not isinstance(
            etl_processes_wrapper_registry,
            EtlProcessesWrapperRegistries):
        raise \
            TypeError

    __report_sub_register(
        sub_register=etl_processes_wrapper_registry.raw_and_bie_sub_register)


def __report_sub_register(
        sub_register: RawAndBieSubRegisters) \
        -> None:
    for table_name_and_origin_type, python_schema_enum_string in sub_register.python_schema_enums.items():
        __export_python_schema_enum(
            python_schema_enum_string=python_schema_enum_string,
            table_name=table_name_and_origin_type[0])


def __export_python_schema_enum(
        python_schema_enum_string: str,
        table_name: str) \
        -> None:
    folder_path = \
        os.path.join(
            LogFiles.folder_path,
            'python_schema_enums')

    os.makedirs(
        folder_path,
        exist_ok=True)

    output_python_file_name = \
        table_name + '_' + '_python_schema_enum.py'

    output_python_file_path = \
        os.path.join(
            folder_path,
            output_python_file_name)

    with open(output_python_file_path, 'w') as f:
        f.write(python_schema_enum_string)
