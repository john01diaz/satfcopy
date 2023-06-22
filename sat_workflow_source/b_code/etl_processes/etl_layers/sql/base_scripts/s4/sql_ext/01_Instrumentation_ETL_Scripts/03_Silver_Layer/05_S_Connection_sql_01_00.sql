
-- MAGIC %sql

 Create Or Replace TEMP VIEW VW_Loop_Tag_DocumentNumber
 As

 Select
 L.loop_database_name
 ,L.CS_loop_element_id as Tag_Number
 ,L.CS_Loop_ID as Loop_Number
 ,DM_Document_Number as Document_Number
 ,L.LC_Item_function_CS_Loop_element_href
 ,L.LC_Item_function_CS_Loop_element_dyn_class
 ,l.CS_manufacturer_mce
 ,l.CS_device_type
 ,Case when loop_dynamic_class='CS_Loop_spez' Then 'Instrumentation'
       when loop_dynamic_class='CS_Electrical_load' Then 'Electrical'
       END as Class
 From sigraph.CS_Layer_Loop_Loop_elements L 
 left outer join (
 Select
 DM_Document_Number
 ,database_name
 ,CS_Loop_DM_Document_dyn_class
 ,CS_Loop_DM_Document_href
 from sigraph.DM_Circuit_diagram 
 qualify Row_Number() over(Partition by database_name,CS_Loop_DM_Document_href order by Cast(DM_modification_time as timestamp) desc) =1
 ) B 
 ON  L.loop_database_name=B.database_name 
 and L.loop_dynamic_class=B.CS_Loop_DM_Document_dyn_class
 and L.loop_object_identifier=B.CS_Loop_DM_Document_href

