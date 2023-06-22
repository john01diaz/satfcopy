-- Databricks notebook source
-- MAGIC %run /Users/p.abhishek@shell.com/01_Instrumentation_ETL_Scripts/06_ETL_Execution_Packages/Databases_Configuration

-- COMMAND ----------

Select Distinct
ClassName
,LoopNo
,LoopFunc
,LService1
,Remarks
,Status
,AreaPath
,Size
,PIDNo
,TagNo
,FormatName
,Area
,Function
,Number 
,suffix
,Device_Type
,OperatingPrinc
,DCSIO
,LoopDwgCode
,ProcessEquipment
,ProcessLines
,ISALocation

,Ex_ID As `Ex-ID`
,Certificate As `Certificate`
,Parts_Code As `Parts Code`
,Facility_Block_Default As `Facility Block Default`
,Location_Block_Default as `Location Block Def.`
,CS_description As `description`
,Model As `Model`
,Suppl_Char_for_Instr_Elements_1 As `Suppl. Char. for Instr. Elements 1`
,Intrinsic_safety_current As `Intrinsic safety current`
,Intrinsic_safety_inductivity As `Intrinsic safety inductivity`
,Intrinsic_safety_capacity As `Intrinsic safety capacity`
,Heating_Settings_Antifreeze As `Heating Settings Antifreeze`
,Suppl_Char_for_Instr_Elements_2 As `Suppl. Char. for Instr. Elements 2`
,Intrinsic_safety_voltage As `Intrinsic safety voltage`
,Intrinsic_safety_power As `Intrinsic safety power`
,Hook_Up_Figure As `Hook-Up Figure`
,Variants_in_Parts As `Variants in Parts`
,Operating_Pressure_at_Feedback_Location_Maximum As `Operating Pressure at Feedback Location Maximum`
,Special_Requirements_ As `Special Requirements`
,Module_Name As `Module Name`
,Installation_Site As `Installation Site`
,Position As `Position`
,Intrinsic_Safety_Device_Type As `Intrinsic Safety Device Type`
,Sample_Sensor As `Sample Sensor`
,Sample_Pipeline_Length As `Sample Pipeline Length`
,Valve_Number As `Valve Number`
,Opening_Closing_Times As `Opening/Closing Times`
,Manufacturer As `Manufacturer`
,Item_Designation_Block_Default As `Item Designation Block Default`
,Run_Out_Length As `Run-Out Length`
,Heating_Limiter As `Heating Limiter`
,Heating_Remark As `Heating Remark`
,Comment As `Comment`
,Overall_Length As `Overall Length`
,dp_Armature_Characteristic_XT As `dp Armature Characteristic XT`
,Number_of_Switching_Contacts As `Number of Switching Contacts`
,Zero_Elevation As `Zero Elevation`
,Differential_Pressure_Range As `Differential Pressure Range`
,Restriction_Orifice_Diameter As `Restriction Orifice Diameter`
,Metering_Cone_Length As `Metering Cone Length`
,Minimum_Design_Temperature As `Minimum Design Temperature`
,Valve_Sizing_Factor_b As `Valve Sizing Factor b`
,Required_Inlet_Length As `Required Inlet Length`
,Red_Marking_Setting As `Red Marking Setting`
,Control_Cable_Length_Heating As `Control Cable Length Heating`
,Capillary_Length As `Capillary Length`
,Pressure_Recover_Factor_FL As `Pressure Recover Factor FL`
,Designation As `Designation`
,Cable_Length_System As `Cable Length System`
,Quarter_Circle_Nozzle_Radius_r As `Quarter Circle Nozzle Radius r`
,Theoretical_Ratio_Kvs_Kvo As `Theoretical Ratio Kvs/Kvo`
,Solenoid_Valve_Design As `Solenoid Valve Design`
,Sample_Probe_Delay_Time As `Sample Probe Delay Time`
,Sample_Preparation_Retention_Time As `Sample Preparation Retention Time`
,Sample_Preparation_Output_Pressure_Normal As `Sample Preparation Output Pressure Normal`
,Sample_Preparation_Output_Pressure_Maximum As `Sample Preparation Output Pressure Maximum`
,Sample_Pipeline_Diameter As `Sample Pipeline Diameter`
,Bypass_Amount_Minimum As `Bypass Amount Minimum`
,Bypass_Amount_Maximum As `Bypass Amount Maximum`
,Analyzer_Range_of_Temperature_Compensation As `Analyzer Range of Temperature Compensation`
,Analyzer_Error_Tolerance As `Analyzer Error Tolerance`
,Calibration_from As `Calibration from`
,PA_Bus_max_Short_Circuit_Current As `PA Bus max. Short-Circuit Current`
,PA_Bus_Current_Input As `PA Bus Current Input`
,Characteristic_Noise_Value_Zy As `Characteristic Noise Value Zy`
,Design_Gauge_Pressure As `Design Gauge Pressure`
,Stroke_Height As `Stroke/Height`
,Effect_of_dp As `Effect of dp`
,Voltage As `Voltage`
,Power_Consumption As `Power Consumption`
,Measuring_Range_from As `Measuring Range from`
,Leakage_Rate_of_Kvs As `Leakage Rate (% of Kvs)`
,Installation_Length As `Installation Length`
,Kv_Value_at_Kvs_Nominal_Stroke As `Kv Value at Kvs Nominal Stroke`
,Measuring_Procedure As `Measuring Procedure`
,Calculated_Maximum_Sound_Pressure_Level As `Calculated Maximum Sound Pressure Level`
,Final_Acceptances As `Final Acceptances`
,Output_Speed As `Output Speed`
,Restrictor_Design As `Restrictor Design`
,Seat_Diameter As `Seat Diameter`
,Insert_Length As `Insert Length`
,Ring_Chamber_Dimension_D As `Ring Chamber Dimension D`
,Control_Device_Design As `Control Device Design`
,Control_Valve_Certification As `Control Valve Certification`
,Kvs_Value As `Kvs-Value`
,Measuring_Range_to As `Measuring Range to`
,Overall_Height As `Overall Height`
,Control_Input_Range As `Control Input Range`
,Calculated_Kv_Value As `Calculated Kv-Value`
,Aperture_Diameter_d As `Aperture Diameter d`
,Calibration_to As `Calibration to`
,Type_of_Drive As `Type of Drive`
,Fail_Safe As `Fail Safe`
,EX_i As `EX (i)`
,Transfer_to_SAP As `Transfer to SAP`
,Circuit_Type_Output As `Circuit Type`
,Power_Supply_from_Module As `Power Supply from Module`
,Classification As `Classification`
,Signal_range_resistance As `Signal range/resistance`
,Method_of_Connection As `Method of Connection`
,Signal_range As `Signal range`
,Signal_Voltage_Input As `Signal Voltage Input`
,Voltage_type As `Voltage type`
,Classification_by As `Classification by`
,Edge_change As `Edge change`
,Signal_Voltage_Output As `Signal Voltage Output`
,Accuracy_Speed_Output As `Accuracy Speed Output ±`
,Actuating_Time As `Actuating Time`
,Actuator_Installation_Position As `Actuator Installation Position`
,Analyzer_Calibration_Curve As `Analyzer Calibration Curve`
,Analyzer_90_time_or_Length_of_Analysis As `Analyzer 90 Percentage time or Length of Analysis`
,Aperture_Angle_Venturi_Tube As `Aperture Angle Venturi Tube`
,Assembly_Category As `Assembly Category`
,Assembly_Identifier As `Assembly Identifier`
,Assembly_Location As `Assembly Location`
,Auxiliary_Materials As `Auxiliary Materials`
,Average_Annual_Temperature As `Average Annual Temperature`
,Bluff_Body_Form As `Bluff Body Form`
,Breakdown_Torque_Ratio_MK_MN As `Breakdown Torque Ratio MK Per MN`
,PTB_Number As `PTB Number`
,Characteristic_Line As `Characteristic Line`
,Length_Connection_Cable As `Length Connection Cable`
,Construction_Year As `Construction Year`
,Control_Cable_Maximum_Number_of_Cable_Inlets As `Control Cable Maximum Number of Cable Inlets`
,Control_Cable_Maximum_Wire_Cross_Section As `Control Cable Maximum Wire Cross Section`
,Controllers_Maximum_Number_of_Cable_Inlets As `Controllers Maximum Number of Cable Inlets`
,Controllers_Maximum_Wire_Cross_Section As `Controllers Maximum Wire Cross Section`
,Control_Tripping_Voltage As `Control Tripping Voltage`
,Controller_Maximum As `Controller Maximum`
,Controller_Minimum As `Controller Minimum`
,Counter As `Counter`
,DN As `DN`
,Cut_Out_Torque As `Cut Out Torque`
,Density_at_15C As `Density at 15C`
,Detonation_Hazard_Protection_Type_Heater As `Detonation Hazard Protection Type Heater`
,Detonation_Hazard_Protection_Type_Motor As `Detonation Hazard Protection Type Motor`
,Rated_Current_of_Drive As `Rated Current of Drive`
,Rated_Power_of_Drive As `Rated Power of Drive`
,Drive_System_for As `Drive System for`
,Efficiency_at_Nominal_Load As `Efficiency at Nominal Load`
,Efficiency_at_Nominal_Power_and_Minimum_Rotational_Speed As `Efficiency at Nominal Power and Minimum Rotational Speed`
,Efficiency_at_Nominal_Power_and_Nominal_Rotational_Speed As `Efficiency at Nominal Power and Nominal Rotational Speed`
,Engine_Identification_Number As `Engine Identification Number`
,Default_Component_Name As `Default Component Name`
,Float_Type_Form As `Float Type Form`
,Frame_Size As `Frame Size`
,Default_Symbol_Library As `Default Symbol Library`
,Default_Symbol As `Default Symbol`
,Heating_Bundle_Diameter As `Heating Bundle Diameter`
,Impulse_Factor As `Impulse Factor`
,Instrument_Layout As `Instrument Layout`
,Instrument_Layout_Page_Number As `Instrument Layout Page Number`
,Interrupting_Current_Limit_xIn As `Interrupting Current Limit xIn`
,Length_of_Displacer As `Length of Displacer`
,Length_of_Suspension_Device As `Length of Suspension Device`
,Length_of_Trace_Heating As `Length of Trace Heating`
,ON_State_Limit_Value_1 As `ON State Limit Value 1`
,ON_State_Limit_Value_2 As `ON State Limit Value 2`
,Limiter_Release As `Limiter Release`
,Setting_Range_Maximum As `Setting Range Maximum`
,Setting_Range_Minimum As `Setting Range Minimum`
,Suppl_char_3_for_loop_elements As `Suppl char 3 for loop elements`
,Sample_Type As `Sample Type`
,Maximum_Cable_Length_of_Converter_Motor As `Maximum Cable Length of Converter Motor`
,Maximum_Coolant_Requirements_at_Nominal_Load As `Maximum Coolant Requirements at Nominal Load`
,Flow_Maximum As `Flow Maximum`
,Maximum_Motor_Power_in_Converter_Operation As `Maximum Motor Power in Converter Operation`
,Maximum_Number_of_Cable_Inlets As `Maximum Number of Cable Inlets`
,Maximum_Power_Loss As `Maximum Power Loss`
,Maximum_Rated_Motor_Current As `Maximum Rated Motor Current`
,Maximum_Rated_Motor_Voltage As `Maximum Rated Motor Voltage`
,Maximum_Permissible_Sound_Pressure_Level As `Maximum Permissible Sound Pressure Level`
,Maximum_Wire_Cross_Section As `Maximum Wire Cross Section`
,Measuring_Range As `Measuring Range`
,Flow_Minimum As `Flow Minimum`
,Minimum_Amount_for_Sale As `Minimum Amount for Sale`
,Motor_Protection_in_Converter_Operation As `Motor Protection in Converter Operation`
,Motor_Protection_in_Network_Operation As `Motor Protection in Network Operation`
,Network_Protection As `Network Protection`
,Noise_Protection_Measures As `Noise Protection Measures`
,Nominal_Power As `Nominal Power`
,Nominal_Torque As `Nominal Torque`
,Maximum_of_Harmonic_Voltage As `Maximum of Harmonic Voltage`
,nth_Time As `nth Time`
,Occupation_Density As `Occupation Density`
,Operating_Pressure_at_Feedback_Location_Normal As `Operating Pressure at Feedback Location Normal`
,Output_Frequency_Control_Range_from As `Output Frequency Control Range from`
,Output_Frequency_Control_Range_to As `Output Frequency Control Range to`
,output_positioner_ As `Output Signal`
,Power_Cable_Maximum_Number_of_Cable_Inlet As `Power Cable Maximum Number of Cable Inlet`
,Power_Cable_Maximum_Wire_Cross_Section As `Power Cable Maximum Wire Cross Section`
,Performance_Factor_at_Full_Load As `Performance Factor at Full Load`
,For_Pre_Post_Pressure_Oscillation As `For Pre Or Post Pressure Oscillation`
,Pressure_Level As `Pressure Level`
,Probe_Length As `Probe Length`
,probe_length_level As `probe length level`
,PTC_Thermistor_Temperature_Sensor As `PTC Thermistor Temperature Sensor`
,Pulse_Number_Input_Line_Side_GR As `Pulse Number Input Line Side GR`
,Pulse_Number_Output_to_Motor_WR As `Pulse Number Output to Motor WR`
,Radio_Interference_Level As `Radio Interference Level`
,Rated_Apparent_Power As `Rated Apparent Power`
,Rated_Current As `Rated Current`
,Rated_Power As `Rated Power`
,Rated_Power_at_Nominal_Operation As `Rated Power at Nominal Operation`
,Rated_Power_per_Meter As `Rated Power per Meter`
,Rated_Asynchronous_Speed As `Rated Asynchronous Speed`
,Rated_Torque_MN As `Rated Torque MN`
,Reference_Temperature As `Reference Temperature`
,Ring_Chamber_Dimension_A As `Ring Chamber Dimension A`
,Ring_Chamber_Dimension_E As `Ring Chamber Dimension E`
,Rotational_Speed_of_Drive As `Rotational Speed of Drive`
,Rotational_Speed_Range_from As `Rotational Speed Range from`
,Sample_Pipeline_Type As `Sample Pipeline Type`
,Sample_Preparation_Back_Pressure_Regulator As `Sample Preparation Back Pressure Regulator`
,Sample_Preparation_Chemical_Template As `Sample Preparation Chemical Template`
,Sample_Preparation_Filter As `Sample Preparation Filter`
,Sample_Preparation_Flow_Counter As `Sample Preparation Flow Counter`
,Sample_Preparation_Measuring_Product_Cooler As `Sample Preparation Measuring Product Cooler`
,Sample_Preparation_Miscellaneous As `Sample Preparation Miscellaneous`
,Sample_Preparation_Reduction_Station As `Sample Preparation Reduction Station`
,Sample_Preparation_Separator As `Sample Preparation Separator`
,Sample_Preparation_Tube_Furnace As `Sample Preparation Tube Furnace`
,Sample_Preparation_Vaporizer As `Sample Preparation Vaporizer`
,Serial_Number As `Serial Number`
,Sound_Pressure_Level_at_No_Load As `Sound Pressure Level at No Load`
,Sound_Pressure_Level_at_Nominal_Load As `Sound Pressure Level at Nominal Load`
,Special_Paint As `Special Paint`
,Rotational_Speed_Range_to As `Rotational Speed Range to`
,Starting_Current_Ratio_IA_IN As `Starting Current Ratio IA Per IN`
,Starting_Torque_Ratio_MA_MN As `Starting Torque Ratio MA Per MN`
,Default_Symbol_Library As `Symbol Library`
,Symbol_Name As `Symbol Name`
,TMU As `TMU`
,Total_Dimensions_Height As `Total Dimensions Height`
,Total_Dimensions_Length As `Total Dimensions Length`
,Total_Dimensions_Width As `Total Dimensions Width`
,Total_Weight As `Total Weight`
,Total_Weight_of_Motor As `Total Weight of Motor`

,Travel_Range As `Travel Range`
,Type_of_Cooling As `Type of Cooling`
,Weight_for_Choke As `Weight for Choke`
,Weight_for_Converter As `Weight for Converter`
,Winding_Circuit As `Winding Circuit`
,Year_of_Construction As `Year of Construction`
,Screwed_Joint As `Screwed Joint`
,Sample_Pipeline_Material As `Sample Pipeline Material`
,Drive_with_Heating As `Drive with Heating`
,Bluff_Body_Material As `Bluff Body Material`
,Seat_Material As `Seat Material`
,Housing_Material As `Housing Material`
,Gland_Nut_Lubrication As `Gland Nut Lubrication`
,Cooling_Fins As `Cooling Fins`
,Standard_Orifice_Nozzle_Material As `Standard Orifice/Nozzle Material`
,Tapping_Material As `Tapping Material`
,Solenoid_Valve As `Solenoid Valve`
,Function_at_Minimum As `Function at Minimum`
,Function_at_Maximum As `Function at Maximum`
,Pressure_Gauge_Shut_Off As `Pressure Gauge Shut-Off`
,Meter_Metering_Element_Material As `Meter/Metering Element Material`
,PA_Bus_Terminal As `PA Bus Terminal`
,Reference_Vessel_Material As `Reference Vessel Material`
,Head_End_Material As `Head End Material`
,Short_Circuit_Proof As `Short-Circuit-Proof`
,Output_Signal_Explosion_Protection As `Output Signal Explosion Protection`
,Input_Signal_Explosion_Protection As `Input Signal Explosion Protection`
,FD_Output_Signal As `FD Output Signal`
,Manual_Actuation As `Manual Actuation`
,Limit_Switch_sv As `Limit Switch`
,Wetted_Body_Material As `Wetted Body Material`
,MT_Head_Assembly As `MT Head Assembly`
,Spindle_Material As `Spindle Material`
,Template_Protection_Fluid As `Template Protection Fluid`
,Restriction_Orifice_Material As `Restriction Orifice Material`
,Metering_Cone_Material As `Metering Cone Material`
,Float_Type_Material As `Float-Type Material`
,Transmitter_Protective_Box As `Transmitter Protective Box`
,Fine_Adjustment_Valve As `Fine Adjustment Valve`
,Pump_for_Sample As `Pump for Sample`
,Pump_for_Sample_Preparation As `Pump for Sample Preparation`
,Primary_Pressure_Reducer As `Primary Pressure Reducer`
,Mass_Limitation_before_AGR As `Mass Limitation before AGR`
,Medium_Connection_Design As `Medium Connection Design`
,Position_without_Power As `Position without Power`
,Loop_Diagram_Level As `Loop Diagram Level`
,Element_type As `Element type`
,Handwheel_Position As `Handwheel Position`
,Measured_Quantity As `Measured Quantity`
,Housing_Type As `Housing Type`
,Signal_type As `Signal type`
,Limit_Switch_signaled As `Limit Switch signaled`
,Digital_Function As `Digital Function`
,Ring_Chamber_Material As `Ring Chamber Material`
,characteristic As `characteristic`
,Auxiliary_power_type As `Auxiliary power type`
,Flow_Direction As `Flow Direction`
,Spindle_Bushing As `Spindle Bushing`
,Resistance_Thermocouple_Type As `Resistance Thermocouple Type`
,Thermometer_Design As `Thermometer Design`
,Extension As `Extension`
,Stelliting As `Stelliting`
,Thermocouple As `Thermocouple`
,Housing_Pressure_Stage As `Housing Pressure Stage`
,Gland_Nut_Type As `Gland Nut Type`
,Input_Signal As `Input Signal`
,Galvanic_Separation As `Galvanic Separation`
,Effective_Direction As `Effective Direction`
,Cone_Type As `Cone Type`
,Electrical_Connection_Design As `Electrical Connection Design`
,Element_fracture As `Element fracture`
,Seat_Sealing As `Seat Sealing`
,Gland_Nut_Material As `Gland Nut Material`
,Auxiliary_Power_Size As `Auxiliary Power Size`
,Cone_Material As `Cone Material`
,Switch_State_at_Required_End_Position As `Switch State at Required End Position`
,Contact_design As `Contact design`
,Orifice_Filling_medium As `Orifice Filling medium`
,Counter_Type As `Counter Type`
,Counter_Design As `Counter Design`
,Control_Cable_Heating As `Control Cable Heating`
,Signal_direction As `Signal direction`
,Scale_Classification As `Scale Classification`
,Meter_Type As `Meter Type`
,Display_type As `Display type`
,Heating As `Heating`
,Filling_Medium_for_Attenuation As `Filling Medium for Attenuation`
,Bypass_for_Motor_Network_Operation As `Bypass for Motor Network Operation`
,Capillary_Liquid As `Capillary Liquid`
,characteristic_positioner As `characteristic positioner`
,Circuit_Arrangement As `Circuit Arrangement`
,Control_Cable_Srewed_Joint As `Control Cable Srewed Joint`
,Detection_Type As `Detection Type`
,Device_Type_Machine_Monitoring As `Device Type Machine Monitoring`
,Enclosure_Type_Class As `Enclosure Type Class`
,External_Setpoint_Specification As `External Setpoint Specification`
,Filter_for_Harmonic_Currents As `Filter for Harmonic Currents`
,Flash_Signaling As `Flash Signaling`
,Heat_Generator_Installation_Position As `Heat Generator Installation Position`
,Installation_Layer As `Installation Layer`
,Insulation_Class As `Insulation Class`
,Limit_Switch_Close As `Limit Switch Close`
,Limit_Switch_Open As `Limit Switch Open`
,Method_of_Measurement_Machine_Monitoring As `Method of Measurement Machine Monitoring`
,Machine_Type As `Machine Type`
,Manual_Operation As `Manual Operation`
,Measured_Value_Display As `Measured Value Display`
,Design As `Design`
,Position_Feedback_Analog As `Position Feedback Analog`
,Power_Cable_Screwed_Joint As `Power Cable Screwed Joint`
,Red_Marking_Design As `Red Marking Design`
,Self_Retaining_Drive As `Self-Retaining Drive`
,Tandem_Switch As `Tandem Switch`
,Temperature_Controller As `Temperature Controller`
,Temperature_Limiter As `Temperature Limiter`
,Terminal_Box_Position_to_drive_end_of_motor As `Terminal Box Position (to drive end of motor)`
,Circuit_Arrangement2 As `Circuit Arrangement2`
,Torque_Dependent As `Torque Dependent`
,Torque_Limit_Switch_Close As `Torque Limit Switch Close`
,Torque_Limit_Switch_Open As `Torque Limit Switch Open`
,Type_of_Starting As `Type of Starting`
,Wiring_Config
,junction_box
,SpecialRemarks
From Sigraph_Silver.S_Instrument_Index
Where Class='Instrumentation' and database_name in (Select Database_name from VW_Database_names)