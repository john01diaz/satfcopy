
-- MAGIC %sql

 Create Or Replace TEMP VIEW VW_Loop_Tag_DocumentNumber
 As

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

 left outer join Sigraph.Loop_Elements LE ON LE.Loop_Element_database_name=L.Loop_database_name
 and LE.CS_Loop_CS_Loop_element_dyn_class=L.loop_dynamic_class
 and LE.CS_Loop_CS_Loop_element_href =L.loop_object_identifier 
 -- Device Type
 left outer join sigraph_reference.DeviceType as DTC ON  UPPER(TRIM(DTC.SigraphDeviceType))=UPPER(TRIM(LE.CS_device_type)) 
 -- Manufacturer
 Left Outer Join sigraph_reference.Manufacturer MC On UPPER(TRIM(MC.Sigraph_manufacturer))=UPPER(TRIM(LE.CS_manufacturer_mce));

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
 From sigraph.PLC_Overview A;

-- MAGIC %sql
 -- Item Classes union- Get the required columns from each Item class
 Create Or Replace Temp View VW_ItemClass
 As
 Select
  B.object_identifier
 ,B.database_name
 ,B.dynamic_class
 ,B.LC_item_complete_part_string
 ,B.LC_item_product_des
 ,B.LC_location_key_dyn_class
 ,B.LC_location_key_href
 ,B.LC_item_fac_rel_dyn_class
 ,B.LC_item_fac_rel_href
 ,B.LC_item_parent_product_key_dyn_class
 ,B.LC_item_parent_product_key_href
 ,'' as LC_plc_item_slot
 ,K.LC_product_key
 ,K.LC_show_key
 ,'Device' as Type
 From sigraph.General_Item B
 Inner join sigraph.Item_K K ON K.database_name=B.database_name and K.LC_Item_k_Rel_dyn_class=B.dynamic_Class and K.LC_Item_k_Rel_href=B.object_identifier
 UNION

 Select
  B.object_identifier
 ,B.database_name
 ,B.dynamic_class
 ,B.LC_item_complete_part_string
 ,B.LC_item_product_des
 ,B.LC_location_key_dyn_class
 ,B.LC_location_key_href
 ,B.LC_item_fac_rel_dyn_class
 ,B.LC_item_fac_rel_href
 ,B.LC_item_parent_product_key_dyn_class
 ,B.LC_item_parent_product_key_href
 ,B.LC_plc_item_slot
 ,K.LC_product_key
 ,K.LC_show_key
 ,Case when Coalesce(A.LC_plc_function_channel,'')<>'' --and Coalesce(A.LC_plc_function_channel,'')<>'0' 
       and PS.object_identifier is not null then 'IO Module'
       When Coalesce(A.LC_plc_function_channel,'')<>'' -- and Coalesce(A.LC_plc_function_channel,'')<>'0' 
       and B.LC_Item_Pin_group_Rel is null then 'IO Module'
       ELSE 'FTA'
       END as Type
 from  sigraph.PLC_Module B 
 Inner join sigraph.Item_K K ON K.database_name=B.database_name and K.LC_Item_k_Rel_dyn_class=B.dynamic_Class and K.LC_Item_k_Rel_href=B.object_identifier
 left outer join sigraph.PLC_Function_special PS ON PS.database_name=B.databasE_name and PS.LC_PLC_Module_PLC_Function_group_href=B.object_identifier 
 left outer join sigraph.PLC_Function A ON A.database_name=B.database_name and A.LC_item_functions_rel_href=B.object_identifier
 UNION

 Select
  B.object_identifier
 ,B.database_name
 ,B.dynamic_class
 ,A.LC_item_function_complete_part_name as LC_item_complete_part_string
 ,B.LC_item_product_des
 ,B.LC_location_key_dyn_class
 ,B.LC_location_key_href
 ,B.LC_item_fac_rel_dyn_class
 ,B.LC_item_fac_rel_href
 ,B.LC_item_parent_product_key_dyn_class
 ,B.LC_item_parent_product_key_href
 ,'' as LC_plc_item_slot
 ,K.LC_product_key
 ,K.LC_show_key
 ,'Terminal Strip' as Type
 From sigraph.Terminal_Strip B
 Inner join sigraph.Item_K K ON K.database_name=B.database_name and K.LC_Item_k_Rel_dyn_class=B.dynamic_Class and K.LC_Item_k_Rel_href=B.object_identifier
 Left outer join sigraph.Component_function A ON A.database_name=B.database_name 
 and A.LC_item_functions_rel_href=B.object_identifier

 UNION

 Select
  B.object_identifier
 ,B.database_name
 ,B.dynamic_class
 ,B.LC_item_complete_part_string
 ,B.LC_item_product_des
 ,B.LC_location_key_dyn_class
 ,B.LC_location_key_href
 ,B.LC_item_fac_rel_dyn_class
 ,B.LC_item_fac_rel_href
 ,B.LC_item_parent_product_key_dyn_class
 ,B.LC_item_parent_product_key_href
 ,'' as LC_plc_item_slot
 ,K.LC_product_key
 ,K.LC_show_key
 ,'Device' as Type
 From sigraph.Integrated_Item B
 Inner join sigraph.Item_K K ON K.database_name=B.database_name and K.LC_Item_k_Rel_dyn_class=B.dynamic_Class and K.LC_Item_k_Rel_href=B.object_identifier;

-- MAGIC %sql
 CREATE OR REPLACE TEMP VIEW VW_ItemFunction_Prep_Query1
 As
 Select Distinct
 A.database_name
 ,A.dynamic_class
 ,A.object_identifier
 ,A.Function_Function_occ_href as Function_Occ_Object_identifier
 ,B.dynamic_class as item_dynamic_class 
 ,B.object_identifier as item_object_identifier 
 ,TRIM(Replace(
       Replace(Replace(Replace(
       Coalesce(B.LC_item_complete_part_string,A.LC_item_function_complete_part_name)
       ,'CC-PIAH01','CC-PAIH01'),'FSC ',''),"'",""),"!",""))
   as  ModelNo
 ,A.Description
 ,A.IOType
 ,A.ChannelNumber
 ,B.LC_item_product_des as product_designation
 ,B.LC_location_key_dyn_class as Location_Dynamic_class
 ,B.LC_location_key_href as Location_Object_Identifier
 ,B.LC_item_fac_rel_dyn_class as Facility_Dynamic_class
 ,B.LC_item_fac_rel_href as Facility_Object_Identifier
 ,B.LC_item_parent_product_key_dyn_class as Rack_Dynamic_class
 ,B.LC_item_parent_product_key_href as Rack_Object_Identifier
 ,B.LC_product_key as Product_Key
 ,B.LC_show_key as Show_Key
 ,B.Type
 ,B.LC_plc_item_slot as Item_Slot
 ,L.LC_location_designation as Location_Designation
 ,MF.LC_facility_designation as Facility_designation
 ,SR.LC_item_product_key as Rack
 ,Symb.Symbol_Name

 From  VW_FunctionClass A
 Inner join VW_ItemClass B ON A.database_name=B.database_name 
 and A.LC_item_functions_rel_dyn_class=B.dynamic_Class
 and A.LC_item_functions_rel_href=B.object_identifier
 -- Location
 Left outer join (
 Select S.object_identifier,S.dynamic_class,S.database_name,LC_location_key as LC_location_designation
 from sigraph.Sub_location S
 UNION
 Select S.object_identifier,S.dynamic_class,S.database_name,S.LC_location_key as LC_location_designation
 from sigraph.Main_location S
 ) as L ON L.database_name=B.database_name and L.dynamic_class=B.LC_location_key_dyn_class and L.object_identifier=B.LC_location_key_href
 -- Facility
 Left outer join (
 Select A.object_identifier,A.database_name,A.dynamic_Class,LC_facility_key as LC_facility_designation
 from sigraph.Sub_facility A
 UNION
 Select A.object_identifier,A.database_name,A.dynamic_Class,A.LC_facility_key as LC_facility_designation
 from sigraph.Main_facility A
 ) as MF on MF.database_name=B.database_name and MF.dynamic_Class=B.LC_item_fac_rel_dyn_class and MF.object_identifier=B.LC_item_fac_rel_href
 -- Rack
 Left outer join sigraph.Subrack SR On SR.database_name=B.database_name and SR.dynamic_Class=B.LC_item_parent_product_key_dyn_class and SR.object_identifier=B.LC_item_parent_product_key_href
 -- symbol name mapping
 left outer join sigraph.Function_occ FCC On A.database_name=FCC.database_name and 
 A.Function_Function_occ_href =FCC.Object_Identifier
 left outer join sigraph.Symbol_def Symb On FCC.database_name=Symb.Database_name and 
 FCC.Symbol_Def_Object_identifier=Symb.Object_Identifier;

-- MAGIC %sql
 CREATE OR REPLACE TEMP VIEW VW_ItemFunction_Prep_Query2
 As
 Select Distinct
 A.database_name
 ,A.dynamic_class
 ,A.object_identifier
 ,A.Function_Occ_Object_identifier
 ,A.item_dynamic_class 
 ,A.item_object_identifier 
 ,A.Location_Dynamic_class
 ,A.Location_Object_Identifier
 ,A.Facility_Dynamic_class
 ,A.Facility_Object_Identifier
 ,A.Rack_Dynamic_class
 ,A.Rack_Object_Identifier
 ,A.ModelNo
 ,A.Description
 ,A.IOType
 ,A.ChannelNumber
 ,A.product_designation
 ,A.Show_Key
 ,A.Item_Slot
 ,A.product_key
 ,Coalesce(B.Tag_Number,C.Tag_Number) as Tag_Number
 ,Coalesce(B.Loop_Number,C.Loop_Number) as Loop_Number
 ,Coalesce(B.Document_Number,C.Document_Number) as Document_Number
 ,Coalesce(B.Class,C.Class,'Inst(Shared)') as Class
 ,A.Type
 -- If the terminal strip is in field, then add the terminal strip tag as a cabinet no or location.
 -- As in Aveva, terminal strip and device cannot be loaded without cabinet no.
 ,Case When Coalesce(B.Tag_Number,C.Tag_Number) is null and A.Location_Designation is null --and Type='Terminal Strip'
       Then A.product_key Else A.Location_Designation END as Location_Designation
 ,Facility_Designation   
 ,Rack
 -- Max is added to have same entry for all the item object identifier's records.
 ,MAX(Coalesce(B.Manufacturer,C.Manufacturer)) Over(Partition by A.database_name,A.Item_Object_Identifier) as Manufacturer
 ,MAX(Coalesce(B.Device_Type,C.Device_Type)) Over(Partition by A.database_name,A.Item_Object_Identifier) as Device_Type
 ,A.Symbol_Name    
 ,Coalesce(B.loop_element_dynamic_class,C.loop_element_dynamic_class) as loop_element_dynamic_class
 ,Coalesce(B.loop_element_Object_Identifier,C.loop_element_Object_Identifier) as loop_element_Object_Identifier
 From VW_ItemFunction_Prep_Query1  A
 -- Ignore Template loop Function items.
 LEFT ANTI JOIN VW_Loop_Tag_DocumentNumber LF ON  A.database_name=LF.loop_database_name
 and A.object_identifier = LF.LC_Item_function_CS_Loop_element_href
 and A.dynamic_class = LF.LC_Item_function_CS_Loop_element_dyn_class
 and LF.Template_Loop='TRUE'

 left outer join VW_Loop_Tag_DocumentNumber B 
 ON  A.database_name=B.loop_database_name
 and A.object_identifier = B.LC_Item_function_CS_Loop_element_href
 and A.dynamic_class = B.LC_Item_function_CS_Loop_element_dyn_class
 -- The instruments where dynamic class and object identifier mapping is not created but we still have that instrument in Sigraph. As per Uli (Sigraph Developer), below logic has been implemented    
 left outer join VW_Loop_Tag_DocumentNumber C 
 ON C.loop_database_name =A.database_name
 and regexp_extract(C.Tag_Number,('^[0-9]+'),0)=A.facility_designation
 and regexp_extract(C.Tag_Number,'([A-Z]+)([0-9]+)',0)=A.product_key
 and C.LC_Item_function_CS_Loop_element_href is null;

CREATE OR REPLACE TEMP VIEW VW_ItemFunction_Prep_Query
As
Select Distinct
A.database_name
,A.dynamic_class
,A.object_identifier
,Function_Occ_Object_identifier
,A.item_dynamic_class
,A.item_object_identifier
,Location_Dynamic_class
,Location_Object_Identifier
,Facility_Dynamic_class
,Facility_Object_Identifier
,Rack_Dynamic_class
,Rack_Object_Identifier
,A.ModelNo
,Description
,IOType
-- The below dense rank condition is added, as for same PLC Module we have Analog and Digital Channels. So it would be duplicate if we try to load only channel numbers + and -, so wherever we have duplicate, we prefixed with IO Type.
,Case When ChannelNumber<>'' and IOType<>''
      and Dense_Rank() Over(partition by A.Database_name,A.item_dynamic_class,A.Dynamic_Class,A.item_object_identifier 
                            order by IOType)>1
      Then Concat(IOType,'_',ChannelNumber)
      Else ChannelNumber END as ChannelNumber
,product_designation
-- If we have same product twice in same cabinet, add the sequence like .1, .2 at the end of the product key
-- Confirmed by Detlef
,Case 
     when location_designation is not null and 
     Dense_Rank(product_key) Over(Partition by location_designation,product_key 
     order by A.database_name,A.Item_Object_identifier)>1 
     Then Concat(product_key,'.',
        Dense_Rank(product_key) Over(Partition by location_designation,product_key 
        order by A.database_name,A.Item_Object_identifier)
                ) 
     Else product_key 
     END as product_key 
,product_key as product_key_Original
,Show_Key
,Item_Slot
,Tag_Number
,Loop_Number
,Document_Number
,Class
,Case When Tag_Number is not null and Location_Designation is null 
      Then 'Field Device' 
      When Tag_Number is not null and Upper(Coalesce(A.device_type,B.device_type,'RHLDD')) like '%INDUCTIVE%SENSOR%'
      Then 'Field Device'
      Else Type
      END as Type
,Location_Designation
,Facility_Designation
,Rack
,Coalesce(A.manufacturer,B.manufacturer,'RHLDD') as manufacturer
,Coalesce(A.device_type,B.device_type,'RHLDD') as device_type
,Coalesce(A.Symbol_Name,B.Symbol_Name) as Symbol_Name
,loop_element_dynamic_class
,loop_element_Object_Identifier
From (
SELECT
A.*
,Count(1) Over(partition by database_name,Item_Dynamic_Class,Item_Object_Identifier)  as NoOfChannels
From  VW_ItemFunction_Prep_Query2 A
-- ignore the unused pins or redundant pins -- Check the email from Jakob on Jan 24 2023
LEFT ANTI JOIN (
Select Distinct
A.database_name,A.dynamic_Class,A.Object_identifier
From VW_ItemFunction_Prep_Query2 A
LEFT SEMI JOIN (
Select Distinct database_name,Item_dynamic_class,Item_Object_Identifier,Product_Key
From VW_ItemFunction_Prep_Query2
Where Type='Field Device'
) as B On A.database_name=B.database_name and A.Item_dynamic_class=B.Item_dynamic_class
and A.Item_Object_Identifier=B.Item_Object_Identifier and A.Product_Key=B.Product_Key
Where Type='Device'
) As RDP ON RDP.database_name=A.database_name and RDP.dynamic_Class=A.dynamic_Class 
and RDP.Object_identifier=A.Object_identifier
) As A

-- If device is nto having symbol name then check below condition and assign the symbol name.
LEFT OUTER JOIN (
Select Distinct
database_name
,dynamic_class
,item_dynamic_class
,Symbol_Name
,ModelNo
,manufacturer
,device_type
,Count(1) Over(partition by database_name,Item_Dynamic_Class,Item_Object_Identifier) as NoOfChannels
FROM VW_ItemFunction_Prep_Query2
where Symbol_Name is not null
) as B On A.database_name=B.database_name and A.dynamic_class=B.dynamic_class and A.item_dynamic_class=B.item_dynamic_class
and A.ModelNo=B.ModelNo and A.NoOfChannels=B.NoOfChannels
-- Get unique record per database_name and object_identifier
Qualify Row_Number() Over(Partition by A.database_name,A.Object_identifier order by A.Object_Identifier)=1;

CREATE OR REPLACE TEMP VIEW VW_ItemFunction
As
Select A.* 
from VW_ItemFunction_Prep_Query A
LEFT ANTI JOIN (
Select F.database_name,F.dynamic_class,F.object_identifier
from VW_ItemFunction_Prep_Query F
LEFT SEMI JOIN VW_ItemFunction_Prep_Query D ON F.database_name=D.database_name 
and F.Item_dynamic_class=D.Item_dynamic_class and F.Item_object_identifier=D.Item_object_identifier 
and D.type='Field Device' and F.Type='Device'
Where  F.database_name='R_2016R3' 
) as B ON A.database_name=B.database_name and A.dynamic_class=B.dynamic_class and A.object_identifier=B.object_identifier
where A.database_name == "R_2016R3";

SELECT DISTINCT Tag_Number from VW_ItemFunction where Type = "Field Device"