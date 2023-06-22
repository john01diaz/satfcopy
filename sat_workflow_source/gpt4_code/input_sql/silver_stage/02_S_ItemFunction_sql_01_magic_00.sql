

-- MAGIC
Create Or Replace TEMP VIEW VW_Loop_Tag_DocumentNumber
As
-- MAGIC
Select Distinct
L.loop_database_name
,L.CS_Loop_ID as Loop_Number
,L.DM_Document_Number as Document_Number
,LE.loop_element_dynamic_class
,LE.loop_element_Object_Identifier
,LE.CS_loop_element_id as Tag_Number
,LE.LC_Item_function_CS_Loop_element_href 
,LE.LC_Item_function_CS_Loop_element_dyn_class 
,Case when loop_dynamic_class='CS_Loop_spez' Then 'Instrumentation'
      when loop_dynamic_class='CS_Electrical_load' Then 'Electrical'
      END as Class
,TRIM(UPPER(Coalesce(Revised_Manufacturer,LE.CS_manufacturer_mce))) as Manufacturer
,TRIM(
       Coalesce(Replace(Replace(Replace(Replace(RevisedDeviceType,'ANALOG INPUT','AI'),'ANALOG OUTPUT','AO')
                 ,'DIGITAL INPUT','DI'),'DIGITAL OUTPUT','DO')
        ,LE.CS_device_type)
        ) as device_type   
,Template_loop        
From sigraph.Loop L 
-- Check if loop is template loop
inner join Sigraph.Layer Layer
on  Layer.database_name      == L.Loop_database_name
and Layer.object_identifier  == L.Layer_CS_Loop_href
and Layer.dynamic_class      == L.Layer_CS_Loop_dyn_class
-- MAGIC
left outer join Sigraph.Loop_Elements LE ON LE.Loop_Element_database_name=L.Loop_database_name
and LE.CS_Loop_CS_Loop_element_dyn_class=L.loop_dynamic_class
and LE.CS_Loop_CS_Loop_element_href =L.loop_object_identifier 
-- Device Type
left outer join sigraph_reference.DeviceType as DTC ON  UPPER(TRIM(DTC.SigraphDeviceType))=UPPER(TRIM(LE.CS_device_type)) 
-- Manufacturer
Left Outer Join sigraph_reference.Manufacturer MC On UPPER(TRIM(MC.Sigraph_manufacturer))=UPPER(TRIM(LE.CS_manufacturer_mce))

