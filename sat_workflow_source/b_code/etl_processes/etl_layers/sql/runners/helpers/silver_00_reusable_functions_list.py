from pyspark.sql.types import StringType

from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_00_Reusable_Functions_v3_cleaning_df_columns_with_enum_display import \
    cleaning_df_columns_with_enum_display
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_00_Reusable_Functions_v3_specified_column import \
    specified_column
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.silver_00_Reusable_Functions_v3_time_it import \
    time_it

silver_00_reusable_functions = \
    [
        # NOTE: None of the following functions have an @udf decorator, nor a spark.udf.register()
        # in silver 00 Reusable Functions
        # TODO: For all "spark.udf.register() not found" functions, the sql name has been set to its function name
        #  in the interim
        # TODO: For all non @udf decorator functions, a return type has been added in the interim
        ('specified_column', specified_column, StringType()),
        # WARNING - it returns a dataframe, not a string
        ('cleaning_df_columns_with_enum_display', cleaning_df_columns_with_enum_display, StringType()),
        # WARNING - it returns a dataframe, not a string
        # ('time_it', time_it, StringType())
        # WARNING - it returns a wrapper, not a string
    ]
