from pyspark.sql.types import ArrayType, StringType

from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_01_Data_Loaders_Functions import \
    format_tag, io_terminal_marking, cleansing_df, io_model_terminal_marking, column_renamed
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_01_Data_Loaders_Functions import get_process_unit
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_01_Data_Loaders_Functions import loop_element_id_suffix
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_01_Data_Loaders_Functions import get_process_unit
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_01_Data_Loaders_Functions import lno_column
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_01_Data_Loaders_Functions import loopsuffix_column
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_01_Data_Loaders_Functions import loopsuffix_column_2
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_01_Data_Loaders_Functions import array_elements_sorting
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_01_Data_Loaders_Functions import get_terminal_marking
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_01_Data_Loaders_Functions import sorted_nicely
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_01_Data_Loaders_Functions import to_get_sort
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_01_Data_Loaders_Functions import to_get_loop_format_tag
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_01_Data_Loaders_Functions import get_colour_revised
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_01_Data_Loaders_Functions import getCableCatalogue
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_01_Data_Loaders_Functions import multiple_replace
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_01_Data_Loaders_Functions import to_get_cable_description
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_01_Data_Loaders_Functions import custom_sort_pairs
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_01_Data_Loaders_Functions import find_group_type

silver_01_data_loaders_functions_and_return_types = \
    [
        # TODO: For all "spark.udf.register() not found" functions, the sql name has been set to its function name
        #  in the interim - Should we even udf.register them at all?
        ('get_process_unit', get_process_unit, StringType()),  # Registered twice in 01 Data Loaders Functions
        ('lno_column', lno_column, StringType()),  # spark.udf.register() not found in 01 Data Loaders Functions
        ('loopsuffix_column', loopsuffix_column, StringType()),  # spark.udf.register() not found in 01 Data Loaders Functions
        ('loopsuffix_column_2', loopsuffix_column_2, StringType()),  # spark.udf.register() not found in 01 Data Loaders Functions
        ('array_elements_sorting', array_elements_sorting, ArrayType(StringType())),
        ('get_terminal_marking', get_terminal_marking, StringType()),
        ('to_get_sort', to_get_sort, StringType()),
        ('to_get_loop_format_tag', to_get_loop_format_tag, StringType()),
        ('ioTerminalMarking', io_terminal_marking, StringType()),
        ('ioModelTerminalMarking', io_model_terminal_marking, StringType()),

        # TODO: Following functions don't have an @udf() decorator - need to investigate their return types
        #  See: https://spark.apache.org/docs/latest/sql-ref-datatypes.html
        #  From chat GPT: it's important to note that while a StructType might represent the structure of a DataFrame,
        #  a DataFrame itself isn't really a type in the same sense as ArrayType, StringType, etc. are types.
        ('formatTag', format_tag, StringType()),
        ('loopelementSuffix', loop_element_id_suffix, StringType()),
        ('sorted_nicely', sorted_nicely, ArrayType(StringType())),  # spark.udf.register() not found in 01 Data Loaders Functions
        ('getColourRevised', get_colour_revised, StringType()),
        ('getCableCatalogue', getCableCatalogue, StringType()),
        ('multipleReplace', multiple_replace, StringType()),
        ('toGetCableDescription', to_get_cable_description, StringType()),
        # ('custom_sort_pairs', custom_sort_pairs, StringType()),  # spark.udf.register() not found in 01 Data Loaders Functions
        # WARNING - returns a list (of lists?)
        ('FindGroupType', find_group_type, StringType()),
        # ('cleansing_df', cleansing_df, StringType()),  # spark.udf.register() not found in 01 Data Loaders Functions
        # WARNING - returns a dataframe
        # ('column_renamed', column_renamed, StringType())  # spark.udf.register() not found in 01 Data Loaders Functions
        # WARNING - returns a dataframe
    ]
