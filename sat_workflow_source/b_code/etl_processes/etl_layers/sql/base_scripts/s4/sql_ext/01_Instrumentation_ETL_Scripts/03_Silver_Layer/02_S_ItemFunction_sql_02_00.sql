
-- MAGIC %sql
 -- Function Classes union- Get the required columns from each function and overview class
 Create Or Replace Temp View VW_FunctionClass
 As
 Select
 A.object_identifier
 ,A.database_name
 ,A.dynamic_class
 ,A.LC_function_def_rel_href
 ,LC_item_function_complete_part_name
 ,A.LC_item_functions_rel_href
 ,A.LC_item_functions_rel_dyn_class
 ,A.Function_Function_occ_href
 ,'RHLDD' as Description
 ,'' as IOType
 ,'' as ChannelNumber
 From sigraph.General_Function A

 UNION

 Select
 A.object_identifier
 ,A.database_name
 ,A.dynamic_class
 ,A.LC_function_def_rel_href
 ,A.LC_item_function_complete_part_name
 ,A.LC_item_functions_rel_href
 ,A.LC_item_functions_rel_dyn_class
 ,A.Function_Function_occ_href
 ,Case when PLC_Function_group_PLC_Function_dyn_class='PLC_Function_digital_output' then 'Digital Output'
       when PLC_Function_group_PLC_Function_dyn_class='PLC_Function_digital_input' then 'Digital Input'
       when PLC_Function_group_PLC_Function_dyn_class='PLC_Function_analogue_output' then 'Analogue Output'
       when PLC_Function_group_PLC_Function_dyn_class='PLC_Function_analogue_input' then 'Analogue Input'
       When A.LC_item_function_complete_part_name in ('CC-PIAH01','SAI-1620m') then 'Analogue Input'
       When A.LC_item_function_complete_part_name in ('CC-PAOH01') then 'Analogue Output'
       When A.LC_item_function_complete_part_name in ('10101/2/1','SDI-1624','CC-PDIL01') then 'Digital Input'
       When A.LC_item_function_complete_part_name in ('CC-PDOB01','SDO-0824') then 'Digital Output'
      When A.LC_item_function_complete_part_name in ('CC-IP0101','RD0-FB-Ex4','HD2-FBPS-1.25.360','CC-PFB401') then 'FIELD BUS'
      ELSE 'RHLDD'
       END  as Description
 ,Case when PLC_Function_group_PLC_Function_dyn_class='PLC_Function_digital_output' then 'DO'
       when PLC_Function_group_PLC_Function_dyn_class='PLC_Function_digital_input' then 'DI'
       when PLC_Function_group_PLC_Function_dyn_class='PLC_Function_analogue_output' then 'AO'
       when PLC_Function_group_PLC_Function_dyn_class='PLC_Function_analogue_input' then 'AI'
       When A.LC_item_function_complete_part_name in ('CC-PIAH01','SAI-1620m') then 'AI'
       When A.LC_item_function_complete_part_name in ('CC-PAOH01') then 'AO'
       When A.LC_item_function_complete_part_name in ('10101/2/1','SDI-1624','CC-PDIL01') then 'DI'
       When A.LC_item_function_complete_part_name in ('CC-PDOB01','SDO-0824') then 'DO'
      When A.LC_item_function_complete_part_name in ('CC-IP0101','RD0-FB-Ex4','HD2-FBPS-1.25.360','CC-PFB401') then 'FIELD BUS'
     END as IOType
 ,LC_plc_function_channel   as ChannelNumber
 From sigraph.PLC_Function A

 UNION

 Select
 A.object_identifier
 ,A.database_name
 ,A.dynamic_class
 ,A.LC_function_def_rel_href
 ,A.LC_item_function_complete_part_name
 ,A.LC_item_functions_rel_href
 ,A.LC_item_functions_rel_dyn_class
 ,A.Function_Function_occ_href
 ,'RHLDD' as Description
 ,'' as IOType
 ,'' as ChannelNumber
 From sigraph.Component_Function A

 UNION

 Select
 A.object_identifier
 ,A.database_name
 ,A.dynamic_class
 ,A.LC_function_def_rel_href
 ,A.LC_item_function_complete_part_name
 ,A.LC_item_functions_rel_href
 ,A.LC_item_functions_rel_dyn_class
 ,A.Function_Function_occ_href
 ,'RHLDD' as Description
 ,'' as IOType
 ,'' as ChannelNumber
 From sigraph.Integrated_function A

 UNION

 Select
 A.object_identifier
 ,A.database_name
 ,A.dynamic_class
 ,A.LC_function_def_rel_href
 ,A.LC_item_function_complete_part_name
 ,A.LC_item_functions_rel_href
 ,A.LC_item_functions_rel_dyn_class
 ,A.Function_Function_occ_href
 ,'RHLDD' as Description
 ,'' as IOType
 ,'' as ChannelNumber
 From sigraph.PLC_Overview A

