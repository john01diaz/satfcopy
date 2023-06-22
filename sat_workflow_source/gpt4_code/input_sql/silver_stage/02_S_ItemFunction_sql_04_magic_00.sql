

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
-- MAGIC
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
FCC.Symbol_Def_Object_identifier=Symb.Object_Identifier

