
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
and A.loop_element_Object_identifier=B.Object_identifier


