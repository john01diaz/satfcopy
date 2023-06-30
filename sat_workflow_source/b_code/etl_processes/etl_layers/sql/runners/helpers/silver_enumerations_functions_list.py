from pyspark.sql.types import StringType

from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.Enumerations_get_enumeration_display_value import get_enumeration_display_value
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.Enumerations_get_loop_picklist_display_value import get_loop_picklist_display_value
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.Enumerations_get_picklist_display_value import get_picklist_display_value
from sat_workflow_source.b_code.etl_processes.etl_layers.python.base_scripts.silver.Enumerations_get_system_enumeration_display_value import get_system_enumeration_display_value

silver_enumerations_functions = \
    [
        # TODO: For all "spark.udf.register() not found" functions, the sql name has been set to its function name
        #  in the interim
        ('get_enumeration_display_value', get_enumeration_display_value, StringType()),  # spark.udf.register() not found in Enumerations.py
        ('get_system_enumeration_display_value', get_system_enumeration_display_value, StringType()),  # # spark.udf.register() not found in Enumerations.py
        ('get_loop_picklist_display_value', get_loop_picklist_display_value, StringType()),
        ('get_picklist_display_value', get_picklist_display_value, StringType())
    ]
