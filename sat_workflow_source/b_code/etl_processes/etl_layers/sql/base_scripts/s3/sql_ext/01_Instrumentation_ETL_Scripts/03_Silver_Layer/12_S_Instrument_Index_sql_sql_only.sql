
CREATE OR REPLACE TEMP VIEW Instrument_List
AS 
SELECT 
 loop_element_database_name
,loop_element_dynamic_class
,loop_element_object_identifier
,'INS' as ClassName

                    ----****---CS_loop_spez columns ----****---
,L.LoopNo as LoopNo
,L.Loop_Function as LoopFunc
,L.Loop_Service_1 as `LService1`
,L.Status_Remark as `Remarks`
,L.Status
,L.Area
,L.AreaPath
,L.Class
                ----***----CS_Mechanical_data_actuator columns----***----
,Size
                ----***----CS_PI_data columns----***----
,concat_ws('-' ,CS_pid_drawing_name,CS_pid_page_number) as PIDNo

                ----***----Loop_ELements columns----***----
,CS_loop_element_id as TagNo
,formatTag(CS_loop_element_id) as FormatName
,regexp_extract(CS_loop_element_id,'([A-Z]+)',1) as Function
,regexp_extract(CS_loop_element_id,'([A-Z]+)([0-9]+)',2) as Number 
,loopelementSuffix(CS_loop_element_id,regexp_extract(CS_loop_element_id,'([0-9]+)',0),regexp_extract(CS_loop_element_id,'([A-Z]+)',1),regexp_extract(CS_loop_element_id,'([A-Z]+)([0-9]+)',2)) as suffix
,CS_device_type as `Device_Type`
,CS_device_type_enum as OperatingPrinc
,case when CS_device_type like '%ANALOG%' and  CS_device_type like '%OUTPUT%' then 'AO'
            when CS_device_type like '%ANALOG%' and  CS_device_type like '%INPUT%' then 'AI'
            when CS_device_type like '%DIGITAL%' or CS_device_type like '%BINARY%' and  CS_device_type like '%OUTPUT%' then 'DO'
            when CS_device_type like '%DIGITAL%' or CS_device_type like '%BINARY%' and  CS_device_type like '%INPUT%' then 'DI'
            else ''
            end as DCSIO
,'' as LoopDwgCode
,'' as ProcessEquipment	
,'' as ProcessLines
,case when CS_location_full_designation is Null then "FLD" else "DCS" end as ISALocation
,CS_location_full_designation as Location_Block_Default
,CS_detonation_hazard_protection_id As Ex_ID
,CS_intrinsic_safety_certificate As Certificate
,ES_part_number As Parts_Code
,CS_facility_full_designation As Facility_Block_Default
,loop_element_CS_description As CS_description
,CS_product_number As Model
,CS_loop_element_id_1 As Suppl_Char_for_Instr_Elements_1
,CS_intrinsic_safety_current As Intrinsic_safety_current
,CS_intrinsic_safety_inductivity As Intrinsic_safety_inductivity
,CS_intrinsic_safety_capacity As Intrinsic_safety_capacity
,CS_heating_settings_antifreeze As Heating_Settings_Antifreeze
,replace(CS_loop_element_id_2, "\n", "") As Suppl_Char_for_Instr_Elements_2
,CS_intrinsic_safety_voltage  As Intrinsic_safety_voltage
,CS_intrinsic_safety_power As Intrinsic_safety_power
,CS_Hook_up_id As Hook_Up_Figure
,ES_part_variant  As Variants_in_Parts
,CS_operating_back_pressure_max  As Operating_Pressure_at_Feedback_Location_Maximum
,CS_special_requirements As Special_Requirements_
,CS_pin_catalogue_id As Module_Name
,CS_mounting_location As Installation_Site
,CS_element_position As Position
,CS_device_type_intrinsic_safety As Intrinsic_Safety_Device_Type
,CS_sampling_probe_quality As Sample_Sensor
,CS_sample_pipeline_length As Sample_Pipeline_Length
,CS_valve_number As Valve_Number
,CS_opening_closing_times As Opening_Closing_Times
,CS_manufacturer As Manufacturer
,CS_item_full_designation As Item_Designation_Block_Default
,CS_outlet_length As Run_Out_Length
,CS_heating_limiter As Heating_Limiter
,CS_heating_remark As Heating_Remark
,replace(loop_element_CS_comment,"\n","") As Comment
,CS_overall_length As Overall_Length
,CS_dp_armature_characteristic As dp_Armature_Characteristic_XT
,CS_switching_contacts_number As Number_of_Switching_Contacts
,CS_zero_elevation As Zero_Elevation
,CS_differential_pressure_range As Differential_Pressure_Range
,CS_restriction_orifice_diameter As Restriction_Orifice_Diameter
,CS_metering_cone_length As Metering_Cone_Length
,CS_designed_temperature As Minimum_Design_Temperature
,CS_valve_sizing_factor As Valve_Sizing_Factor_b
,CS_required_inlet_length As Required_Inlet_Length
,CS_red_marking_setting As Red_Marking_Setting
,CS_control_cable_length_heating As Control_Cable_Length_Heating
,CS_capillary_length As Capillary_Length
,CS_pressure_gain_factor As Pressure_Recover_Factor_FL
,CS_pin_catalogue_description As Designation
,CS_system_cable_length As Cable_Length_System
,CS_quarter_circle_nozzle_radius As Quarter_Circle_Nozzle_Radius_r
,CS_theor_ratio_kvs_kvo As Theoretical_Ratio_Kvs_Kvo
,CS_solenoid_valve_design As Solenoid_Valve_Design
,CS_sample_probe_time_delay As Sample_Probe_Delay_Time
,CS_sample_preparation_retention_time As Sample_Preparation_Retention_Time
,CS_sample_preparation_output_pressure_norm As Sample_Preparation_Output_Pressure_Normal
,CS_sample_preparation_output_pressure_max As Sample_Preparation_Output_Pressure_Maximum
,CS_sample_pipeline_diameter As Sample_Pipeline_Diameter
,CS_bypass_amount_min As Bypass_Amount_Minimum
,CS_bypass_amount_max As Bypass_Amount_Maximum
,CS_analyzer_range_temperature_compensation As Analyzer_Range_of_Temperature_Compensation
,CS_analyzer_error_margin As Analyzer_Error_Tolerance
,CS_calibration_from As Calibration_from
,CS_pa_bus_max_short_circuit_current As PA_Bus_max_Short_Circuit_Current
,CS_pa_bus_current_input As PA_Bus_Current_Input
,CS_characteristic_noise_value As Characteristic_Noise_Value_Zy
,CS_designed_pressure As Design_Gauge_Pressure
,CS_stroke_height As Stroke_Height
,CS_effect_dp As Effect_of_dp
,CS_voltage As Voltage
,CS_power_consumption As Power_Consumption
,CS_measuring_range_from As Measuring_Range_from
,CS_leakage_rate_kvs As Leakage_Rate_of_Kvs
,CS_insertion_length_thermowell As Installation_Length
,CS_kv_value_kvs_nominal As Kv_Value_at_Kvs_Nominal_Stroke
,CS_measuring_procedure As Measuring_Procedure
,loop_element_CS_calc_max_sound_pressure_level As Calculated_Maximum_Sound_Pressure_Level
,CS_final_acceptance As Final_Acceptances
,CS_output_speed As Output_Speed
,CS_restrictor_design As Restrictor_Design
,CS_seat_diameter As Seat_Diameter
,CS_insert_length As Insert_Length
,CS_ring_chamber_dimension_D As Ring_Chamber_Dimension_D
,CS_control_valve_design As Control_Device_Design
,CS_control_valve_certification As Control_Valve_Certification
,CS_kvs_value As Kvs_Value
,CS_measuring_range_to As Measuring_Range_to
,CS_overall_height As Overall_Height
,CS_control_input_range As Control_Input_Range
,CS_calculated_kv_value As Calculated_Kv_Value
,CS_aperture_diameter_d As Aperture_Diameter_d
,CS_calibration_to As Calibration_to
,CS_drive_type As Type_of_Drive
,CS_failsafe_hw As Fail_Safe
,CS_ex_i As EX_i
,SAP_transfer As Transfer_to_SAP
,CS_conn_type_analog_output As Circuit_Type_Output
,CS_voltage_supply_module As Power_Supply_from_Module
,loop_element_CS_classification As Classification
,CS_sig_range_ai As Signal_range_resistance
,CS_conn_type_analog_input As Method_of_Connection
,CS_sig_range_ao As Signal_range
,CS_sig_voltage_digital_input As Signal_Voltage_Input
,loop_element_CS_voltage_type As Voltage_type
,loop_element_CS_classification_by As Classification_by
,CS_edge_change As Edge_change
,CS_sig_voltage_digital_output As Signal_Voltage_Output
,CS_accuracy_speed_output As Accuracy_Speed_Output
,CS_actuating_time As Actuating_Time
,CS_actuator_installation_position As Actuator_Installation_Position
,CS_analyzer_calibration_curve As Analyzer_Calibration_Curve
,CS_analyzer_time_length_of_analysis As Analyzer_90_time_or_Length_of_Analysis
,CS_aperture_angle_venturi_tube As Aperture_Angle_Venturi_Tube
,CS_assembly_category As Assembly_Category
,CS_assembly_identifier As Assembly_Identifier
,CS_assembly_location As Assembly_Location
,CS_auxiliary_materials As Auxiliary_Materials
,CS_average_temp As Average_Annual_Temperature
,CS_bluff_body_form As Bluff_Body_Form
,CS_breakdown_torque_ratio As Breakdown_Torque_Ratio_MK_MN
,CS_certificate As PTB_Number
,CS_characteristic_line As Characteristic_Line
,CS_conn_cable_length As Length_Connection_Cable
,CS_construction_year As Construction_Year
,CS_control_cable_max_number_cable_inlet As Control_Cable_Maximum_Number_of_Cable_Inlets
,CS_control_cable_max_wire_cross_section As Control_Cable_Maximum_Wire_Cross_Section
,CS_control_max_number_cable_inlets As Controllers_Maximum_Number_of_Cable_Inlets
,CS_control_max_wire_cross_section As Controllers_Maximum_Wire_Cross_Section
,CS_control_tripping_voltage As Control_Tripping_Voltage
,CS_controller_max As Controller_Maximum
,CS_controller_min As Controller_Minimum
,CS_counter As Counter
,CS_counter_dn As DN
,CS_cutoff_torque As Cut_Out_Torque
,CS_density_at_temp As Density_at_15C
,CS_detonation_hazard_protection_type_heat As Detonation_Hazard_Protection_Type_Heater
,CS_detonation_hazard_protection_type_motor As Detonation_Hazard_Protection_Type_Motor
,CS_drive_auxiliary_current As Rated_Current_of_Drive
,CS_drive_power As Rated_Power_of_Drive
,CS_drive_system As Drive_System_for
,CS_efficiency_factor_nominal_load As Efficiency_at_Nominal_Load
,CS_efficiency_min_speed As Efficiency_at_Nominal_Power_and_Minimum_Rotational_Speed
,CS_efficiency_rated_speed As Efficiency_at_Nominal_Power_and_Nominal_Rotational_Speed
,CS_engine_identification_number As Engine_Identification_Number
,CS_equipment_name As Default_Component_Name
,CS_float_type_form As Float_Type_Form
,CS_frame_size As Frame_Size
,CS_given_symbol_library As Default_Symbol_Library
,CS_given_symbol_name As Default_Symbol
,CS_heating_bundle_diameter As Heating_Bundle_Diameter
,CS_impulse_factor As Impulse_Factor
,CS_instrument_layout As Instrument_Layout
,CS_instrument_layout_page_number As Instrument_Layout_Page_Number
,CS_interrupting_current_limit_xln As Interrupting_Current_Limit_xIn
,CS_length_displacer As Length_of_Displacer
,CS_length_suspension_device As Length_of_Suspension_Device
,CS_length_trace_heating As Length_of_Trace_Heating
,CS_limit_value_1 As ON_State_Limit_Value_1
,CS_limit_value_2 As ON_State_Limit_Value_2
,CS_limiter_release As Limiter_Release
,CS_limiter_setting_max As Setting_Range_Maximum
,CS_limiter_setting_min As Setting_Range_Minimum
,CS_loop_element_id_3 As Suppl_char_3_for_loop_elements
,CS_loop_element_sample_type As Sample_Type
,CS_max_cable_length_converter_motor As Maximum_Cable_Length_of_Converter_Motor
,CS_max_coolant_requirement_nominal_load As Maximum_Coolant_Requirements_at_Nominal_Load
,CS_max_flow As Flow_Maximum
,CS_max_motor_power_converter_operation As Maximum_Motor_Power_in_Converter_Operation
,CS_max_number_cable_inlets As Maximum_Number_of_Cable_Inlets
,CS_max_power_loss As Maximum_Power_Loss
,CS_max_rated_motor_current As Maximum_Rated_Motor_Current
,CS_max_rated_motor_voltage As Maximum_Rated_Motor_Voltage
,CS_max_sound_pressure_level As Maximum_Permissible_Sound_Pressure_Level
,CS_max_wire_cross_section As Maximum_Wire_Cross_Section
,CS_measuring_range As Measuring_Range
,CS_min_flow As Flow_Minimum
,CS_min_sale_amount As Minimum_Amount_for_Sale
,CS_motor_protection_converter_operation As Motor_Protection_in_Converter_Operation
,CS_motor_protection_network_operation As Motor_Protection_in_Network_Operation
,CS_network_protection As Network_Protection
,CS_noise_protection_measures As Noise_Protection_Measures
,CS_nominal_force As Nominal_Power
,CS_nominal_torque As Nominal_Torque
,CS_norm_of_max_harmonic_voltage As Maximum_of_Harmonic_Voltage
,CS_nth_time As nth_Time
,CS_occupation_density As Occupation_Density
,CS_operating_back_pressure_norm As Operating_Pressure_at_Feedback_Location_Normal
,CS_output_freq_control_range_from As Output_Frequency_Control_Range_from
,CS_output_freq_control_range_to As Output_Frequency_Control_Range_to
,CS_output_positioner As output_positioner_
,CS_power_cable_max_number_cable_inlet As Power_Cable_Maximum_Number_of_Cable_Inlet
,CS_power_cable_max_wire_cross_section As Power_Cable_Maximum_Wire_Cross_Section
,CS_power_factor_full_load As Performance_Factor_at_Full_Load
,CS_prepressure_postpressure_oscillation As For_Pre_Post_Pressure_Oscillation
,CS_pressure_level As Pressure_Level
,CS_probe_length As Probe_Length
,CS_probe_length_level As probe_length_level
,CS_ptc_thermistor_temp_sensor As PTC_Thermistor_Temperature_Sensor
,CS_pulse_number_input As Pulse_Number_Input_Line_Side_GR
,CS_pulse_number_output As Pulse_Number_Output_to_Motor_WR
,CS_radio_interference_level As Radio_Interference_Level
,CS_rated_apparent_power As Rated_Apparent_Power
,CS_rated_current As Rated_Current
,CS_rated_power As Rated_Power
,CS_rated_power_nominal_operation As Rated_Power_at_Nominal_Operation
,CS_rated_power_per_meter As Rated_Power_per_Meter
,CS_rated_speed_asynchronous As Rated_Asynchronous_Speed
,CS_rated_torque As Rated_Torque_MN
,CS_reference_temp As Reference_Temperature
,CS_ring_chamber_dimension_A As Ring_Chamber_Dimension_A
,CS_ring_chamber_dimension_E As Ring_Chamber_Dimension_E
,CS_rotational_speed_drive As Rotational_Speed_of_Drive
,CS_rotational_speed_range_from As Rotational_Speed_Range_from
,CS_sample_pipeline_type As Sample_Pipeline_Type
,CS_sample_preparation_back_pressure_regulator As Sample_Preparation_Back_Pressure_Regulator
,CS_sample_preparation_chem_template As Sample_Preparation_Chemical_Template
,CS_sample_preparation_filter As Sample_Preparation_Filter
,CS_sample_preparation_flow_counter As Sample_Preparation_Flow_Counter
,CS_sample_preparation_measuring_product_cooler As Sample_Preparation_Measuring_Product_Cooler
,CS_sample_preparation_miscellaneous As Sample_Preparation_Miscellaneous
,CS_sample_preparation_reduction_station As Sample_Preparation_Reduction_Station
,CS_sample_preparation_separator As Sample_Preparation_Separator
,CS_sample_preparation_tube_furnace As Sample_Preparation_Tube_Furnace
,CS_sample_preparation_vaporizer As Sample_Preparation_Vaporizer
,CS_serial_number As Serial_Number
,CS_sound_pressure_level_no_load As Sound_Pressure_Level_at_No_Load
,CS_sound_pressure_level_nominal_load As Sound_Pressure_Level_at_Nominal_Load
,CS_special_paint As Special_Paint
,CS_speed_range_to As Rotational_Speed_Range_to
,CS_starting_current_ratio As Starting_Current_Ratio_IA_IN
,CS_starting_torque_ratio As Starting_Torque_Ratio_MA_MN
,CS_symbol_name As Symbol_Name
,CS_tmu As TMU
,CS_total_height As Total_Dimensions_Height
,CS_total_length As Total_Dimensions_Length
,CS_total_weight As Total_Weight
,CS_total_weight_motor As Total_Weight_of_Motor
,CS_total_width As Total_Dimensions_Width
,CS_travel_range As Travel_Range
,CS_type_cooling As Type_of_Cooling
,CS_weight_choke As Weight_for_Choke
,CS_weight_converter As Weight_for_Converter
,CS_winding_circuit As Winding_Circuit
,CS_year_of_construction As Year_of_Construction
,SC_screwed_joint As Screwed_Joint

-- Picklist Properties

,CS_sample_pipeline_material As Sample_Pipeline_Material
,CS_drive_with_heating As Drive_with_Heating
,CS_bluff_body_material As Bluff_Body_Material
,CS_seat_material As Seat_Material
,CS_housing_material As Housing_Material
,CS_gland_nut_lubrication As Gland_Nut_Lubrication
,CS_cooling_fins As Cooling_Fins
,CS_standard_orifice_nozzle_material As Standard_Orifice_Nozzle_Material
,CS_tapping_material As Tapping_Material
,CS_solenoid_valve As Solenoid_Valve
,CS_function_at_min As Function_at_Minimum
,CS_function_at_max As Function_at_Maximum
,CS_pressure_gauge_shutoff As Pressure_Gauge_Shut_Off
,CS_meter_metering_element_material As Meter_Metering_Element_Material
,CS_pa_bus_terminal As PA_Bus_Terminal
,CS_reference_vessel_material As Reference_Vessel_Material
,CS_head_end_material As Head_End_Material
,CS_short_circ_proof As Short_Circuit_Proof
,CS_explosion_prot_output_signal As Output_Signal_Explosion_Protection
,CS_explosion_prot_input_signal As Input_Signal_Explosion_Protection
,CS_fd_out_sig As FD_Output_Signal
,CS_manual_actuation As Manual_Actuation
,CS_limit_switch_sv As Limit_Switch_sv
,CS_wetted_body_material As Wetted_Body_Material
,CS_transmitter_head_assembly As MT_Head_Assembly
,CS_spindle_material As Spindle_Material
,CS_template_protection_fluid As Template_Protection_Fluid
,CS_restriction_orifice_material As Restriction_Orifice_Material
,CS_metering_cone_material As Metering_Cone_Material
,CS_float_type_material As Float_Type_Material
,CS_transmitter_protective_box As Transmitter_Protective_Box
,CS_fine_adjustment_valve As Fine_Adjustment_Valve
,CS_sampling_probe_pump As Pump_for_Sample
,CS_sample_preparation_pump As Pump_for_Sample_Preparation
,CS_primary_pressure_reducer As Primary_Pressure_Reducer
,CS_mass_limitation As Mass_Limitation_before_AGR
,CS_medium_connection_design As Medium_Connection_Design
,CS_position_without_power As Position_without_Power
,CS_level As Loop_Diagram_Level
,CS_intrinsic_safety_elm_type As Element_type
,CS_handwheel_position As Handwheel_Position
,CS_measurand As Measured_Quantity
,CS_body_type As Housing_Type
,CS_signal_kind As Signal_type
,CS_limit_switch_signaled As Limit_Switch_signaled
,CS_digital_function As Digital_Function
,CS_ring_chamber_material As Ring_Chamber_Material
,CS_ss_characteristic As characteristic
,CS_auxiliary_power_type As Auxiliary_power_type
,CS_flow_direction As Flow_Direction
,CS_spindle_bushing As Spindle_Bushing
,CS_thermocouple_type As Resistance_Thermocouple_Type
,CS_thermometer_design As Thermometer_Design
,CS_mounting_type As Extension
,CS_stellited_part As Stelliting
,CS_thermocouple As Thermocouple
,CS_housing_pressure_stage As Housing_Pressure_Stage
,CS_gland_nut_type As Gland_Nut_Type
,CS_fd_inp_sig As Input_Signal
,CS_met_isol As Galvanic_Separation
,CS_effective_direction As Effective_Direction
,CS_cone_type As Cone_Type
,CS_termination_type As Electrical_Connection_Design
,CS_element_fracture As Element_fracture
,CS_seat_sealing As Seat_Sealing
,CS_gland_nut_material As Gland_Nut_Material
,CS_auxiliary_power_size As Auxiliary_Power_Size
,CS_cone_material As Cone_Material
,CS_switch_state_end_position As Switch_State_at_Required_End_Position
,CS_contact_arrangement As Contact_design
,CS_orifice_filling_medium As Orifice_Filling_medium
,CS_counter_model As Counter_Type
,CS_counter_design As Counter_Design
,CS_control_cable_heating As Control_Cable_Heating
,CS_signal_direction As Signal_direction
,CS_scale_marking As Scale_Classification
,CS_meter_type As Meter_Type
,CS_display_type As Display_type
,CS_heating As Heating
,CS_filling_medium As Filling_Medium_for_Attenuation
,CS_bypass_motor_network_operation As Bypass_for_Motor_Network_Operation
,CS_capillary_liquid As Capillary_Liquid
,CS_characteristic_positioner As characteristic_positioner
,CS_con_type As Circuit_Arrangement
,CS_control_cable_screwed_joint As Control_Cable_Srewed_Joint
,CS_detection_type As Detection_Type
,CS_device_type_mach_mon As Device_Type_Machine_Monitoring
,CS_enclosure_type_class As Enclosure_Type_Class
,CS_external_setpoint_speed As External_Setpoint_Specification
,CS_filter_harmonic_currents As Filter_for_Harmonic_Currents
,CS_flash_signaling As Flash_Signaling
,CS_heat_generator_installation_position As Heat_Generator_Installation_Position
,CS_installation_layer As Installation_Layer
,CS_insulation_class As Insulation_Class
,CS_limit_switch_close As Limit_Switch_Close
,CS_limit_switch_open As Limit_Switch_Open
,CS_mach_mon_measure_method As Method_of_Measurement_Machine_Monitoring
,CS_machine_type As Machine_Type
,CS_manual_operation As Manual_Operation
,CS_measurand_value_disp As Measured_Value_Display
,CS_motor_design As Design
,CS_position_feedback_analog As Position_Feedback_Analog
,CS_power_cable_screwed_joint As Power_Cable_Screwed_Joint
,CS_red_marking_design As Red_Marking_Design
,CS_self_retaining_drive As Self_Retaining_Drive
,CS_tandem_switch As Tandem_Switch
,CS_temperature_controller As Temperature_Controller
,CS_temperature_limiter As Temperature_Limiter
,CS_terminal_box_pos As Terminal_Box_Position_to_drive_end_of_motor
,CS_therm_conn_type As Circuit_Arrangement2
,CS_torque_dependent As Torque_Dependent
,CS_torque_limit_switch_close As Torque_Limit_Switch_Close
,CS_torque_limit_switch_open As Torque_Limit_Switch_Open
,CS_type_starting AS `Type_of_Starting`
                   ---G_FIELD_DEVICE_CATALOGUE columns---
,`Catalogue_Name` as `Wiring_Config`
                    ---JB_Allocation---
,`junction box` as junction_box
,Case when `Catalogue_Name`='' Then 'Mechanical Connection. Need to be added in the drawing' END as SpecialRemarks
from Sigraph.Loop_Elements LE
Inner join Sigraph_Silver.S_Loop_Index L On LE.loop_element_database_name=L.database_name 
and LE.CS_Loop_CS_Loop_element_dyn_class=L.dynamic_class
and LE.CS_Loop_CS_Loop_element_href=L.object_identifier
-- Restrict the data only for field device
INNER join sigraph_silver.S_Field_Device_Catalogue  FD 
on  FD.database_name==LE.loop_element_database_name
and FD.dynamic_class=LE.loop_element_dynamic_class
and FD.Object_Identifier=LE.loop_element_Object_Identifier
-- Mechanical Data
LEFT JOIN
        (Select distinct 
            database_name
            ,concat_ws(',',collect_set(CS_pipeline_main_nominal_size) over(partition by CS_Loop_CS_Mechanical_data_href)) as size
            ,CS_Loop_CS_Mechanical_data_dyn_class
            ,CS_Loop_CS_Mechanical_data_href
        from  sigraph.CS_Mechanical_data_actuator
        ) CS_Mechanical_data_actuator
on  L.database_name     == CS_Mechanical_data_actuator.database_name
and L.dynamic_class     == CS_Mechanical_data_actuator.CS_Loop_CS_Mechanical_data_dyn_class
and L.object_identifier == CS_Mechanical_data_actuator.CS_Loop_CS_Mechanical_data_href
-- PI Data
LEFT JOIN
        (Select distinct 
            database_name
            ,CS_PI_data_CS_Loop_dyn_class
            ,CS_PI_data_CS_Loop_href
            ,CS_pid_drawing_name
            ,CS_pid_page_number 
        FROM sigraph.CS_PI_data 
        )CS_PI_data
on  L.database_name     == CS_PI_data.database_name
and L.dynamic_class     == CS_PI_data.CS_PI_data_CS_Loop_dyn_class
and L.object_identifier == CS_PI_data.CS_PI_data_CS_Loop_href
-- If the instrument is assigned to an JB, then provide the JB details
Left outer join (     
      Select 
      To_Location as `junction box` 
      ,Tag_Number,Loop_Number
      ,database_name
      from sigraph_silver.S_Connection 
      Where From_location is null 
      and To_item_Dynamic_Class='LC_Terminal_strip'
      --and  regexp_extract(To_Location,'[A-Za-z]+',0) in ('IC','IE','IH','IM','IP','IQ','IS','IX','IY','IZ')
      UNION
      Select 
      From_Location
      ,Tag_Number
      ,Loop_Number
      ,database_name
      from sigraph_silver.S_Connection 
      Where   To_location is null 
      and From_item_Dynamic_Class='LC_Terminal_strip'
     -- and regexp_extract(From_Location,'[A-Za-z]+',0) in ('IC','IE','IH','IM','IP','IQ','IS','IX','IY','IZ')
      ) as JB_Allocation 
ON  JB_Allocation.database_name==LE.loop_element_database_name
-- and JB_Allocation.Loop_Number=loop.CS_loop_id
and JB_Allocation.Tag_Number=LE.CS_loop_element_id;

-- Soft tags data loading script
CREATE OR REPLACE TEMP VIEW VW_SoftTag_Instrument_List
AS 
Select  Distinct
A.database_name
,A.Dynamic_Class
,A.object_identifier
,'VINS' as ClassName
,'As Built' as Status
,'Default' as Area
,'' as AreaPath
,'Instrumentation' as Class
,Tag_Number as TagNo
,'RHLND Free Form' as FormatName
,'Soft tag' as Device_Type
,'Soft tag' as OperatingPrinc
,'FLD' as ISALocation
,Junction_Box
,'Soft tag' as SpecialRemarks
from sigraph_silver.S_IO_Allocations  A
Where IsSoftTag=1;

-- Union of Instrument and Soft tags
CREATE OR REPLACE TEMP VIEW Instrument_List_Final
AS 
Select Distinct
Coalesce(A.loop_element_database_name,B.database_name) as database_name
,Coalesce(A.loop_element_dynamic_class,B.dynamic_class) as dynamic_class
,Coalesce(A.loop_element_object_identifier,B.object_identifier) as object_identifier
,Coalesce(A.ClassName,B.ClassName) as ClassName
,LoopNo
,LoopFunc
,LService1
,Remarks
,Coalesce(A.Status		,B.Status	) as Status
,Coalesce(A.Area        ,B.Area     ) as Area
,Coalesce(A.AreaPath    ,B.AreaPath ) as AreaPath
,Coalesce(A.Class       ,B.Class    ) as Class
,Size
,PIDNo
,Coalesce(A.TagNo,B.TagNo) as TagNo
,Coalesce(A.FormatName,B.FormatName) as FormatName
,Function
,Number
,suffix
,Coalesce(A.Device_Type,B.Device_Type) as Device_Type
,Coalesce(A.OperatingPrinc,B.OperatingPrinc) as OperatingPrinc
,DCSIO
,LoopDwgCode
,ProcessEquipment
,ProcessLines
,Coalesce(A.ISALocation,B.ISALocation) as ISALocation
,Location_Block_Default
,Ex_ID
,Certificate
,Parts_Code
,Facility_Block_Default
,CS_description
,Model
,Suppl_Char_for_Instr_Elements_1
,Intrinsic_safety_current
,Intrinsic_safety_inductivity
,Intrinsic_safety_capacity
,Heating_Settings_Antifreeze
,Suppl_Char_for_Instr_Elements_2
,Intrinsic_safety_voltage
,Intrinsic_safety_power
,Hook_Up_Figure
,Variants_in_Parts
,Operating_Pressure_at_Feedback_Location_Maximum
,Special_Requirements_
,Module_Name
,Installation_Site
,Position
,Intrinsic_Safety_Device_Type
,Sample_Sensor
,Sample_Pipeline_Length
,Valve_Number
,Opening_Closing_Times
,Manufacturer
,Item_Designation_Block_Default
,Run_Out_Length
,Heating_Limiter
,Heating_Remark
,Comment
,Overall_Length
,dp_Armature_Characteristic_XT
,Number_of_Switching_Contacts
,Zero_Elevation
,Differential_Pressure_Range
,Restriction_Orifice_Diameter
,Metering_Cone_Length
,Minimum_Design_Temperature
,Valve_Sizing_Factor_b
,Required_Inlet_Length
,Red_Marking_Setting
,Control_Cable_Length_Heating
,Capillary_Length
,Pressure_Recover_Factor_FL
,Designation
,Cable_Length_System
,Quarter_Circle_Nozzle_Radius_r
,Theoretical_Ratio_Kvs_Kvo
,Solenoid_Valve_Design
,Sample_Probe_Delay_Time
,Sample_Preparation_Retention_Time
,Sample_Preparation_Output_Pressure_Normal
,Sample_Preparation_Output_Pressure_Maximum
,Sample_Pipeline_Diameter
,Bypass_Amount_Minimum
,Bypass_Amount_Maximum
,Analyzer_Range_of_Temperature_Compensation
,Analyzer_Error_Tolerance
,Calibration_from
,PA_Bus_max_Short_Circuit_Current
,PA_Bus_Current_Input
,Characteristic_Noise_Value_Zy
,Design_Gauge_Pressure
,Stroke_Height
,Effect_of_dp
,Voltage
,Power_Consumption
,Measuring_Range_from
,Leakage_Rate_of_Kvs
,Installation_Length
,Kv_Value_at_Kvs_Nominal_Stroke
,Measuring_Procedure
,Calculated_Maximum_Sound_Pressure_Level
,Final_Acceptances
,Output_Speed
,Restrictor_Design
,Seat_Diameter
,Insert_Length
,Ring_Chamber_Dimension_D
,Control_Device_Design
,Control_Valve_Certification
,Kvs_Value
,Measuring_Range_to
,Overall_Height
,Control_Input_Range
,Calculated_Kv_Value
,Aperture_Diameter_d
,Calibration_to
,Type_of_Drive
,Fail_Safe
,EX_i
,Transfer_to_SAP
,Circuit_Type_Output
,Power_Supply_from_Module
,Classification
,Signal_range_resistance
,Method_of_Connection
,Signal_range
,Signal_Voltage_Input
,Voltage_type
,Classification_by
,Edge_change
,Signal_Voltage_Output
,Accuracy_Speed_Output
,Actuating_Time
,Actuator_Installation_Position
,Analyzer_Calibration_Curve
,Analyzer_90_time_or_Length_of_Analysis
,Aperture_Angle_Venturi_Tube
,Assembly_Category
,Assembly_Identifier
,Assembly_Location
,Auxiliary_Materials
,Average_Annual_Temperature
,Bluff_Body_Form
,Breakdown_Torque_Ratio_MK_MN
,PTB_Number
,Characteristic_Line
,Length_Connection_Cable
,Construction_Year
,Control_Cable_Maximum_Number_of_Cable_Inlets
,Control_Cable_Maximum_Wire_Cross_Section
,Controllers_Maximum_Number_of_Cable_Inlets
,Controllers_Maximum_Wire_Cross_Section
,Control_Tripping_Voltage
,Controller_Maximum
,Controller_Minimum
,Counter
,DN
,Cut_Out_Torque
,Density_at_15C
,Detonation_Hazard_Protection_Type_Heater
,Detonation_Hazard_Protection_Type_Motor
,Rated_Current_of_Drive
,Rated_Power_of_Drive
,Drive_System_for
,Efficiency_at_Nominal_Load
,Efficiency_at_Nominal_Power_and_Minimum_Rotational_Speed
,Efficiency_at_Nominal_Power_and_Nominal_Rotational_Speed
,Engine_Identification_Number
,Default_Component_Name
,Float_Type_Form
,Frame_Size
,Default_Symbol_Library
,Default_Symbol
,Heating_Bundle_Diameter
,Impulse_Factor
,Instrument_Layout
,Instrument_Layout_Page_Number
,Interrupting_Current_Limit_xIn
,Length_of_Displacer
,Length_of_Suspension_Device
,Length_of_Trace_Heating
,ON_State_Limit_Value_1
,ON_State_Limit_Value_2
,Limiter_Release
,Setting_Range_Maximum
,Setting_Range_Minimum
,Suppl_char_3_for_loop_elements
,Sample_Type
,Maximum_Cable_Length_of_Converter_Motor
,Maximum_Coolant_Requirements_at_Nominal_Load
,Flow_Maximum
,Maximum_Motor_Power_in_Converter_Operation
,Maximum_Number_of_Cable_Inlets
,Maximum_Power_Loss
,Maximum_Rated_Motor_Current
,Maximum_Rated_Motor_Voltage
,Maximum_Permissible_Sound_Pressure_Level
,Maximum_Wire_Cross_Section
,Measuring_Range
,Flow_Minimum
,Minimum_Amount_for_Sale
,Motor_Protection_in_Converter_Operation
,Motor_Protection_in_Network_Operation
,Network_Protection
,Noise_Protection_Measures
,Nominal_Power
,Nominal_Torque
,Maximum_of_Harmonic_Voltage
,nth_Time
,Occupation_Density
,Operating_Pressure_at_Feedback_Location_Normal
,Output_Frequency_Control_Range_from
,Output_Frequency_Control_Range_to
,output_positioner_
,Power_Cable_Maximum_Number_of_Cable_Inlet
,Power_Cable_Maximum_Wire_Cross_Section
,Performance_Factor_at_Full_Load
,For_Pre_Post_Pressure_Oscillation
,Pressure_Level
,Probe_Length
,probe_length_level
,PTC_Thermistor_Temperature_Sensor
,Pulse_Number_Input_Line_Side_GR
,Pulse_Number_Output_to_Motor_WR
,Radio_Interference_Level
,Rated_Apparent_Power
,Rated_Current
,Rated_Power
,Rated_Power_at_Nominal_Operation
,Rated_Power_per_Meter
,Rated_Asynchronous_Speed
,Rated_Torque_MN
,Reference_Temperature
,Ring_Chamber_Dimension_A
,Ring_Chamber_Dimension_E
,Rotational_Speed_of_Drive
,Rotational_Speed_Range_from
,Sample_Pipeline_Type
,Sample_Preparation_Back_Pressure_Regulator
,Sample_Preparation_Chemical_Template
,Sample_Preparation_Filter
,Sample_Preparation_Flow_Counter
,Sample_Preparation_Measuring_Product_Cooler
,Sample_Preparation_Miscellaneous
,Sample_Preparation_Reduction_Station
,Sample_Preparation_Separator
,Sample_Preparation_Tube_Furnace
,Sample_Preparation_Vaporizer
,Serial_Number
,Sound_Pressure_Level_at_No_Load
,Sound_Pressure_Level_at_Nominal_Load
,Special_Paint
,Rotational_Speed_Range_to
,Starting_Current_Ratio_IA_IN
,Starting_Torque_Ratio_MA_MN
,Symbol_Name
,TMU
,Total_Dimensions_Height
,Total_Dimensions_Length
,Total_Weight
,Total_Weight_of_Motor
,Total_Dimensions_Width
,Travel_Range
,Type_of_Cooling
,Weight_for_Choke
,Weight_for_Converter
,Winding_Circuit
,Year_of_Construction
,Screwed_Joint
,Sample_Pipeline_Material
,Drive_with_Heating
,Bluff_Body_Material
,Seat_Material
,Housing_Material
,Gland_Nut_Lubrication
,Cooling_Fins
,Standard_Orifice_Nozzle_Material
,Tapping_Material
,Solenoid_Valve
,Function_at_Minimum
,Function_at_Maximum
,Pressure_Gauge_Shut_Off
,Meter_Metering_Element_Material
,PA_Bus_Terminal
,Reference_Vessel_Material
,Head_End_Material
,Short_Circuit_Proof
,Output_Signal_Explosion_Protection
,Input_Signal_Explosion_Protection
,FD_Output_Signal
,Manual_Actuation
,Limit_Switch_sv
,Wetted_Body_Material
,MT_Head_Assembly
,Spindle_Material
,Template_Protection_Fluid
,Restriction_Orifice_Material
,Metering_Cone_Material
,Float_Type_Material
,Transmitter_Protective_Box
,Fine_Adjustment_Valve
,Pump_for_Sample
,Pump_for_Sample_Preparation
,Primary_Pressure_Reducer
,Mass_Limitation_before_AGR
,Medium_Connection_Design
,Position_without_Power
,Loop_Diagram_Level
,Element_type
,Handwheel_Position
,Measured_Quantity
,Housing_Type
,Signal_type
,Limit_Switch_signaled
,Digital_Function
,Ring_Chamber_Material
,characteristic
,Auxiliary_power_type
,Flow_Direction
,Spindle_Bushing
,Resistance_Thermocouple_Type
,Thermometer_Design
,Extension
,Stelliting
,Thermocouple
,Housing_Pressure_Stage
,Gland_Nut_Type
,Input_Signal
,Galvanic_Separation
,Effective_Direction
,Cone_Type
,Electrical_Connection_Design
,Element_fracture
,Seat_Sealing
,Gland_Nut_Material
,Auxiliary_Power_Size
,Cone_Material
,Switch_State_at_Required_End_Position
,Contact_design
,Orifice_Filling_medium
,Counter_Type
,Counter_Design
,Control_Cable_Heating
,Signal_direction
,Scale_Classification
,Meter_Type
,Display_type
,Heating
,Filling_Medium_for_Attenuation
,Bypass_for_Motor_Network_Operation
,Capillary_Liquid
,characteristic_positioner
,Circuit_Arrangement
,Control_Cable_Srewed_Joint
,Detection_Type
,Device_Type_Machine_Monitoring
,Enclosure_Type_Class
,External_Setpoint_Specification
,Filter_for_Harmonic_Currents
,Flash_Signaling
,Heat_Generator_Installation_Position
,Installation_Layer
,Insulation_Class
,Limit_Switch_Close
,Limit_Switch_Open
,Method_of_Measurement_Machine_Monitoring
,Machine_Type
,Manual_Operation
,Measured_Value_Display
,Design
,Position_Feedback_Analog
,Power_Cable_Screwed_Joint
,Red_Marking_Design
,Self_Retaining_Drive
,Tandem_Switch
,Temperature_Controller
,Temperature_Limiter
,Terminal_Box_Position_to_drive_end_of_motor
,Circuit_Arrangement2
,Torque_Dependent
,Torque_Limit_Switch_Close
,Torque_Limit_Switch_Open
,Type_of_Starting
,Wiring_Config
,Coalesce(A.junction_box,B.junction_box) as junction_box
,Coalesce(A.SpecialRemarks,B.SpecialRemarks) as SpecialRemarks
,Case When A.loop_element_Object_identifier is not null Then 'Instrument' Else 'Soft tag' End as Data_Type
from Instrument_List A
FULL OUTER JOIN VW_SoftTag_Instrument_List B ON A.loop_element_database_name=B.database_name 
and A.loop_element_Object_identifier=B.Object_identifier;

-- -- Restricted with only limited properties, as per Zahra request on 12/12/2022
-- Select 
-- Area
-- ,AreaPath
-- ,ClassName
-- ,FormatName
-- ,TagNo
-- ,Function
-- ,Number
-- ,LoopFunc
-- ,suffix
-- ,LoopNo
-- ,LService1
-- ,ISALocation
-- ,Status
-- ,`Device_Type`
-- ,Instrument_Service
-- ,manufacturer_mce
-- ,ModelNo
-- ,ProcessEquipment
-- ,ProcessLines
-- ,Size
-- ,PIDNo
-- ,LoopDwgCode
-- ,DCSIO
-- ,OperatingPrinc
-- ,Remarks
-- ,`Wiring_Config`
-- ,`junction_box`
-- ,Case when `Wiring_Config`='' Then 'Mechanical Connection. Need to be added in the drawing' END as SpecialRemarks
-- from sigraph_silver.S_Instrument_Index