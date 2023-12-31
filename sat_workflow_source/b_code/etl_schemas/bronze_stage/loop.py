from enum import Enum


class Loop(
        Enum):
    CS_ACCOMPANYING_DOCUMENTS = 'cs_accompanying_documents'
    CS_AIR_DISTRIBUTOR = 'cs_air_distributor'
    CS_AMBIENT_TEMPERATURE_MAX = 'cs_ambient_temperature_max'
    CS_AMBIENT_TEMPERATURE_MAX_DYN_CLASS = 'cs_ambient_temperature_max_dyn_class'
    CS_AMBIENT_TEMPERATURE_MAX_HREF = 'cs_ambient_temperature_max_href'
    CS_AMBIENT_TEMPERATURE_MIN = 'cs_ambient_temperature_min'
    CS_AMBIENT_TEMPERATURE_MIN_DYN_CLASS = 'cs_ambient_temperature_min_dyn_class'
    CS_AMBIENT_TEMPERATURE_MIN_HREF = 'cs_ambient_temperature_min_href'
    CS_COMMENT_MOUNTING_CONDITION = 'cs_comment_mounting_condition'
    CS_COMMENT_MOUNTING_CONDITION_DYN_CLASS = 'cs_comment_mounting_condition_dyn_class'
    CS_COMMENT_MOUNTING_CONDITION_HREF = 'cs_comment_mounting_condition_href'
    CS_COMMENT_NETWORK = 'cs_comment_network'
    CS_COMMENT_NETWORK_DYN_CLASS = 'cs_comment_network_dyn_class'
    CS_COMMENT_NETWORK_HREF = 'cs_comment_network_href'
    CS_CONSUMER_TYPE = 'cs_consumer_type'
    CS_CONSUMER_TYPE_ENUM = 'cs_consumer_type_enum'
    CS_DANGER_ZONE = 'cs_danger_zone'
    CS_DANGER_ZONE_DYN_CLASS = 'cs_danger_zone_dyn_class'
    CS_DANGER_ZONE_ENUM = 'cs_danger_zone_enum'
    CS_DANGER_ZONE_HREF = 'cs_danger_zone_href'
    CS_DESCRIPTION_DYN_CLASS = 'cs_description_dyn_class'
    CS_DESCRIPTION_HREF = 'cs_description_href'
    CS_DETONATION_HAZARD_PROTECTION_TYPE = 'cs_detonation_hazard_protection_type'
    CS_DETONATION_HAZARD_PROTECTION_TYPE_DYN_CLASS = 'cs_detonation_hazard_protection_type_dyn_class'
    CS_DETONATION_HAZARD_PROTECTION_TYPE_HREF = 'cs_detonation_hazard_protection_type_href'
    CS_ELECTRICAL_LOAD_CS_MOUNTING_CONDITIONS_DYN_CLASS = 'cs_electrical_load_cs_mounting_conditions_dyn_class'
    CS_ELECTRICAL_LOAD_CS_MOUNTING_CONDITIONS_HREF = 'cs_electrical_load_cs_mounting_conditions_href'
    CS_ELECTRICAL_LOAD_CS_NETWORK_TYPE_DYN_CLASS = 'cs_electrical_load_cs_network_type_dyn_class'
    CS_ELECTRICAL_LOAD_CS_NETWORK_TYPE_HREF = 'cs_electrical_load_cs_network_type_href'
    CS_ELECTRICAL_LOAD_MACHINE_TYPE = 'cs_electrical_load_machine_type'
    CS_ET_NODE_PATH = 'cs_et_node_path'
    CS_EXPLOSION_GROUP = 'cs_explosion_group'
    CS_EXPLOSION_GROUP_DYN_CLASS = 'cs_explosion_group_dyn_class'
    CS_EXPLOSION_GROUP_HREF = 'cs_explosion_group_href'
    CS_FAILSAFE = 'cs_failsafe'
    CS_FAILSAFE_DYN_CLASS = 'cs_failsafe_dyn_class'
    CS_FAILSAFE_ENUM = 'cs_failsafe_enum'
    CS_FAILSAFE_HREF = 'cs_failsafe_href'
    CS_FIELD_DISTRIBUTOR_INPUT_ID = 'cs_field_distributor_input_id'
    CS_FIELD_DISTRIBUTOR_OUTPUT_ID = 'cs_field_distributor_output_id'
    CS_FREQUENCY_TOLERANCE = 'cs_frequency_tolerance'
    CS_FREQUENCY_TOLERANCE_DYN_CLASS = 'cs_frequency_tolerance_dyn_class'
    CS_FREQUENCY_TOLERANCE_HREF = 'cs_frequency_tolerance_href'
    CS_FUNCTION_DESCRIPTION = 'cs_function_description'
    CS_FUNCTION_TEST = 'cs_function_test'
    CS_GRAPHIC_TYPICAL = 'cs_graphic_typical'
    CS_HEAVY_STARTING = 'cs_heavy_starting'
    CS_HEAVY_STARTING_ENUM = 'cs_heavy_starting_enum'
    CS_INERTIA_TORQUE_COMPLETE = 'cs_inertia_torque_complete'
    CS_INSPECTION_INTERVAL = 'cs_inspection_interval'
    CS_INSPECTION_PURPOSE = 'cs_inspection_purpose'
    CS_LOAD_TYPE = 'cs_load_type'
    CS_LOAD_TYPE_ENUM = 'cs_load_type_enum'
    CS_LOGIC_DIAGRAM_TYPICAL = 'cs_logic_diagram_typical'
    CS_LOOP_CS_INTRINSIC_SAFETY_LOOP = 'cs_loop_cs_intrinsic_safety_loop'
    CS_LOOP_CS_KEY_DYN_CLASS = 'cs_loop_cs_key_dyn_class'
    CS_LOOP_CS_KEY_HREF = 'cs_loop_cs_key_href'
    CS_LOOP_CS_LOOP_ELEMENT = 'cs_loop_cs_loop_element'
    CS_LOOP_CS_LOOP_SUBFUNCTION = 'cs_loop_cs_loop_subfunction'
    CS_LOOP_CS_MECHANICAL_DATA = 'cs_loop_cs_mechanical_data'
    CS_LOOP_CS_PROCESS_DATA = 'cs_loop_cs_process_data'
    CS_LOOP_CS_PROCESS_FLOW_DIAGRAM = 'cs_loop_cs_process_flow_diagram'
    CS_LOOP_CS_PROCESS_FLOW_DIAGRAM_DYN_CLASS = 'cs_loop_cs_process_flow_diagram_dyn_class'
    CS_LOOP_CS_PROCESS_FLOW_DIAGRAM_HREF = 'cs_loop_cs_process_flow_diagram_href'
    CS_LOOP_CS_TERMINAL_LEVEL = 'cs_loop_cs_terminal_level'
    CS_LOOP_DESCRIPTION_1 = 'cs_loop_description_1'
    CS_LOOP_DESCRIPTION_2 = 'cs_loop_description_2'
    CS_LOOP_DESCRIPTION_3 = 'cs_loop_description_3'
    CS_LOOP_DM_DOCUMENT = 'cs_loop_dm_document'
    CS_LOOP_ELEMENT_HW_CONTROL_VALVE_CS_LOOP_SPEZ_DYN_CLASS = 'cs_loop_element_hw_control_valve_cs_loop_spez_dyn_class'
    CS_LOOP_ELEMENT_HW_CONTROL_VALVE_CS_LOOP_SPEZ_HREF = 'cs_loop_element_hw_control_valve_cs_loop_spez_href'
    CS_LOOP_FUNCTION = 'cs_loop_function'
    CS_LOOP_ID = 'cs_loop_id'
    CS_LOOP_ID_1 = 'cs_loop_id_1'
    CS_LOOP_ID_2 = 'cs_loop_id_2'
    CS_LOOP_ID_3 = 'cs_loop_id_3'
    CS_LOOP_ID_DYN_CLASS = 'cs_loop_id_dyn_class'
    CS_LOOP_ID_HREF = 'cs_loop_id_href'
    CS_LOOP_KAT_ID = 'cs_loop_kat_id'
    CS_LOOP_LFD_NR = 'cs_loop_lfd_nr'
    CS_LOOP_STATUS = 'cs_loop_status'
    CS_LOOP_STATUS_ENUM = 'cs_loop_status_enum'
    CS_LOOP_STATUS_REMARK = 'cs_loop_status_remark'
    CS_LOOP_UNIT_DYN_CLASS = 'cs_loop_unit_dyn_class'
    CS_LOOP_UNIT_HREF = 'cs_loop_unit_href'
    CS_MACHINE_ID = 'cs_machine_id'
    CS_MACHINE_MANUFACTURER = 'cs_machine_manufacturer'
    CS_MOTOR_OPERATION_MODE = 'cs_motor_operation_mode'
    CS_MOTOR_OPERATION_MODE_ENUM = 'cs_motor_operation_mode_enum'
    CS_MOUNTING_AREA = 'cs_mounting_area'
    CS_MOUNTING_AREA_DYN_CLASS = 'cs_mounting_area_dyn_class'
    CS_MOUNTING_AREA_HREF = 'cs_mounting_area_href'
    CS_MOUNTING_HEIGHT = 'cs_mounting_height'
    CS_MOUNTING_HEIGHT_DYN_CLASS = 'cs_mounting_height_dyn_class'
    CS_MOUNTING_HEIGHT_HREF = 'cs_mounting_height_href'
    CS_NETWORK_NEUTRAL_POINT = 'cs_network_neutral_point'
    CS_NETWORK_NEUTRAL_POINT_DYN_CLASS = 'cs_network_neutral_point_dyn_class'
    CS_NETWORK_NEUTRAL_POINT_ENUM = 'cs_network_neutral_point_enum'
    CS_NETWORK_NEUTRAL_POINT_HREF = 'cs_network_neutral_point_href'
    CS_NETWORK_TYPE_ID = 'cs_network_type_id'
    CS_NETWORK_TYPE_ID_DYN_CLASS = 'cs_network_type_id_dyn_class'
    CS_NETWORK_TYPE_ID_HREF = 'cs_network_type_id_href'
    CS_OPERATING_TYPE = 'cs_operating_type'
    CS_PHASES_CONDUCTORS_ARRANGEMENT = 'cs_phases_conductors_arrangement'
    CS_PHASES_CONDUCTORS_ARRANGEMENT_DYN_CLASS = 'cs_phases_conductors_arrangement_dyn_class'
    CS_PHASES_CONDUCTORS_ARRANGEMENT_HREF = 'cs_phases_conductors_arrangement_href'
    CS_PI_DATA_CS_LOOP = 'cs_pi_data_cs_loop'
    CS_POWER_TRANSMISSION = 'cs_power_transmission'
    CS_POWER_TRANSMISSION_ENUM = 'cs_power_transmission_enum'
    CS_RATED_FREQUENCY = 'cs_rated_frequency'
    CS_RATED_FREQUENCY_DYN_CLASS = 'cs_rated_frequency_dyn_class'
    CS_RATED_FREQUENCY_HREF = 'cs_rated_frequency_href'
    CS_RATED_MOTOR_POWER_MAX = 'cs_rated_motor_power_max'
    CS_RATED_MOTOR_POWER_MAX_DYN_CLASS = 'cs_rated_motor_power_max_dyn_class'
    CS_RATED_MOTOR_POWER_MAX_HREF = 'cs_rated_motor_power_max_href'
    CS_RATED_MOTOR_POWER_MIN = 'cs_rated_motor_power_min'
    CS_RATED_MOTOR_POWER_MIN_DYN_CLASS = 'cs_rated_motor_power_min_dyn_class'
    CS_RATED_MOTOR_POWER_MIN_HREF = 'cs_rated_motor_power_min_href'
    CS_RATED_POWER_PN_REQUESTED_1 = 'cs_rated_power_pn_requested_1'
    CS_RATED_POWER_PN_REQUESTED_2 = 'cs_rated_power_pn_requested_2'
    CS_RATED_POWER_PN_REQUESTED_3 = 'cs_rated_power_pn_requested_3'
    CS_RATED_VOLTAGE_DYN_CLASS = 'cs_rated_voltage_dyn_class'
    CS_RATED_VOLTAGE_HREF = 'cs_rated_voltage_href'
    CS_REALIZATION_POSITION = 'cs_realization_position'
    CS_RELATED_REPORT = 'cs_related_report'
    CS_RELATED_REPORT_ENUM = 'cs_related_report_enum'
    CS_RELATIVE_HUMIDITY = 'cs_relative_humidity'
    CS_RELATIVE_HUMIDITY_DYN_CLASS = 'cs_relative_humidity_dyn_class'
    CS_RELATIVE_HUMIDITY_HREF = 'cs_relative_humidity_href'
    CS_ROOT_EXTRACTION_POINT = 'cs_root_extraction_point'
    CS_ROOT_EXTRACTION_POINT_ENUM = 'cs_root_extraction_point_enum'
    CS_ROTATIONAL_SPEED_MAX = 'cs_rotational_speed_max'
    CS_ROTATIONAL_SPEED_MIN = 'cs_rotational_speed_min'
    CS_ROTATIONAL_SPEED_REQUESTED_1 = 'cs_rotational_speed_requested_1'
    CS_ROTATIONAL_SPEED_REQUESTED_2 = 'cs_rotational_speed_requested_2'
    CS_ROTATIONAL_SPEED_REQUESTED_3 = 'cs_rotational_speed_requested_3'
    CS_SHAFT_POWER_100_PERCENT_1 = 'cs_shaft_power_100_percent_1'
    CS_SHAFT_POWER_100_PERCENT_2 = 'cs_shaft_power_100_percent_2'
    CS_SHAFT_POWER_100_PERCENT_3 = 'cs_shaft_power_100_percent_3'
    CS_SHAFT_POWER_REQUIRED_1 = 'cs_shaft_power_required_1'
    CS_SHAFT_POWER_REQUIRED_2 = 'cs_shaft_power_required_2'
    CS_SHAFT_POWER_REQUIRED_3 = 'cs_shaft_power_required_3'
    CS_SHORT_CIRCUIT_POWER_SK_MIN = 'cs_short_circuit_power_sk_min'
    CS_SHORT_CIRCUIT_POWER_SK_MIN_DYN_CLASS = 'cs_short_circuit_power_sk_min_dyn_class'
    CS_SHORT_CIRCUIT_POWER_SK_MIN_HREF = 'cs_short_circuit_power_sk_min_href'
    CS_SIFPRO_RELEVANT = 'cs_sifpro_relevant'
    CS_SIFPRO_RELEVANT_ENUM = 'cs_sifpro_relevant_enum'
    CS_SIMULTANEITY_FACTOR = 'cs_simultaneity_factor'
    CS_SOURCE = 'cs_source'
    CS_SOURCE_ENUM = 'cs_source_enum'
    CS_SPECIAL_MOTOR = 'cs_special_motor'
    CS_SPECIAL_REQUIREMENTS_1 = 'cs_special_requirements_1'
    CS_SPECIAL_REQUIREMENTS_1_ENUM = 'cs_special_requirements_1_enum'
    CS_SPECIAL_REQUIREMENTS_2 = 'cs_special_requirements_2'
    CS_SPECIAL_REQUIREMENTS_2_ENUM = 'cs_special_requirements_2_enum'
    CS_SPECIAL_REQUIREMENTS_3 = 'cs_special_requirements_3'
    CS_SPECIAL_REQUIREMENTS_3_ENUM = 'cs_special_requirements_3_enum'
    CS_STANDARD_LOOP_ID = 'cs_standard_loop_id'
    CS_TEMPERATURE_CLASS = 'cs_temperature_class'
    CS_TEMPERATURE_CLASS_DYN_CLASS = 'cs_temperature_class_dyn_class'
    CS_TEMPERATURE_CLASS_ENUM = 'cs_temperature_class_enum'
    CS_TEMPERATURE_CLASS_HREF = 'cs_temperature_class_href'
    CS_TEST_ACC_BY_TEST_CATALOG = 'cs_test_acc_by_test_catalog'
    CS_TEST_ACC_TO_TEST_CATALOG = 'cs_test_acc_to_test_catalog'
    CS_TEST_SECTION = 'cs_test_section'
    CS_TESTED_BY = 'cs_tested_by'
    CS_TORQUE_CHARACTERISTIC_MACHINE = 'cs_torque_characteristic_machine'
    CS_TORQUE_REQUIRED_1 = 'cs_torque_required_1'
    CS_TORQUE_REQUIRED_2 = 'cs_torque_required_2'
    CS_TORQUE_REQUIRED_3 = 'cs_torque_required_3'
    CS_VARIABLE_PART_DRAWING_NUMBER = 'cs_variable_part_drawing_number'
    CS_VISUAL_INSPECTION = 'cs_visual_inspection'
    CS_VOLTAGE_DEVIATION_STATIC_TO_10S_DYN_CLASS = 'cs_voltage_deviation_static_to_10s_dyn_class'
    CS_VOLTAGE_DEVIATION_STATIC_TO_10S_HREF = 'cs_voltage_deviation_static_to_10s_href'
    CS_VOLTAGE_DEVIATION_STATIONARY = 'cs_voltage_deviation_stationary'
    CS_VOLTAGE_DEVIATION_STATIONARY_DYN_CLASS = 'cs_voltage_deviation_stationary_dyn_class'
    CS_VOLTAGE_DEVIATION_STATIONARY_HREF = 'cs_voltage_deviation_stationary_href'
    CS_VOLTAGE_DEVIATION_TEMP_TO_24H_DYN_CLASS = 'cs_voltage_deviation_temp_to_24h_dyn_class'
    CS_VOLTAGE_DEVIATION_TEMP_TO_24H_HREF = 'cs_voltage_deviation_temp_to_24h_href'
    CS_VOLTAGE_TYPE_DYN_CLASS = 'cs_voltage_type_dyn_class'
    CS_VOLTAGE_TYPE_HREF = 'cs_voltage_type_href'
    CS_X_COORDINATE_PI_DATA = 'cs_x_coordinate_pi_data'
    CS_Y_COORDINATE_PI_DATA = 'cs_y_coordinate_pi_data'
    DM_DOCUMENT_NUMBER = 'dm_document_number'
    LAYER_CS_LOOP_DYN_CLASS = 'layer_cs_loop_dyn_class'
    LAYER_CS_LOOP_HREF = 'layer_cs_loop_href'
    LAYER_CS_LOOP_REF_DYN_CLASS = 'layer_cs_loop_ref_dyn_class'
    LAYER_CS_LOOP_REF_HREF = 'layer_cs_loop_ref_href'
    LOOP_CS_CALC_MAX_SOUND_PRESSURE_LEVEL = 'loop_cs_calc_max_sound_pressure_level'
    LOOP_CS_CLASSIFICATION = 'loop_cs_classification'
    LOOP_CS_CLASSIFICATION_BY = 'loop_cs_classification_by'
    LOOP_CS_CLASSIFICATION_BY_ENUM = 'loop_cs_classification_by_enum'
    LOOP_CS_CLASSIFICATION_ENUM = 'loop_cs_classification_enum'
    LOOP_CS_COMMENT = 'loop_cs_comment'
    LOOP_CS_DESCRIPTION = 'loop_cs_description'
    LOOP_CS_LOOP_UNIT = 'loop_cs_loop_unit'
    LOOP_CS_OPERATING_TYPE_ENUM = 'loop_cs_operating_type_enum'
    LOOP_CS_RATED_VOLTAGE = 'loop_cs_rated_voltage'
    LOOP_CS_ROTATIONAL_DIRECTION = 'loop_cs_rotational_direction'
    LOOP_CS_ROTATIONAL_DIRECTION_ENUM = 'loop_cs_rotational_direction_enum'
    LOOP_CS_VOLTAGE_TYPE = 'loop_cs_voltage_type'
    LOOP_CS_VOLTAGE_TYPE_ENUM = 'loop_cs_voltage_type_enum'
    LOOP_DATABASE_NAME = 'loop_database_name'
    LOOP_DYNAMIC_CLASS = 'loop_dynamic_class'
    LOOP_OBJECT_IDENTIFIER = 'loop_object_identifier'
    LOOP_SAP_FUNCTION = 'loop_sap_function'
    LOOP_SAP_HIERARCHY_LEVEL = 'loop_sap_hierarchy_level'
    LOOP_SAP_REMARK = 'loop_sap_remark'
    LOOP_SAP_SHORT_TEXT = 'loop_sap_short_text'
    LOOP_SAP_TRANSFER_STATUS = 'loop_sap_transfer_status'
    PA_CHECKED_OUT = 'pa_checked_out'
    PARQUET_FILE_RELATIVE_PATH = 'parquet_file_relative_path'
    PDM_ABS_ET_NODE_CS_LOOP_REL_DYN_CLASS = 'pdm_abs_et_node_cs_loop_rel_dyn_class'
    PDM_ABS_ET_NODE_CS_LOOP_REL_HREF = 'pdm_abs_et_node_cs_loop_rel_href'
    SAP_DEPARTMENT = 'sap_department'
    SAP_FUNCTIONAL_LOCATION_LEVEL_4 = 'sap_functional_location_level_4'
    SAP_PLANNING_GROUP = 'sap_planning_group'
    SAP_PLANT_SECTION = 'sap_plant_section'
    SAP_RESPONSIBLE_WORK_CENTER = 'sap_responsible_work_center'
    SAP_SUPERIOR_FUNCTIONAL_LOCATION = 'sap_superior_function'
