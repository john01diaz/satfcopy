
-- MAGIC %sql
 CREATE OR Replace TEMP VIEW VW_IO_Module
 AS

 Select Distinct
  A.database_name
 ,A.object_identifier
 ,A.dynamic_class
 ,A.location_designation as Parent_Equipment_No
 ,A.product_key as Equipment_No
 ,'IO Module' as EquipmentType
 ,B.ModelNo as CatalogueNo
 ,A.Rack
 ,A.Item_Object_identifier
 ,A.Item_dynamic_class
 ,Case When Coalesce(A.item_slot,'')<>'' and Coalesce(A.item_slot,'')<>'0' then item_slot END as Slot
 ,A.Class
 from sigraph_silver.S_Itemfunction A
 Inner join sigraph_silver.S_Item_Function_Model B 
 On A.database_name=B.database_name
 and A.Item_Dynamic_Class=B.Item_Dynamic_Class
 and A.Item_Object_identifier=B.Item_Object_identifier
 and A.Dynamic_Class=B.Dynamic_Class
 and A.Object_Identifier=B.Object_Identifier
 Where A.Type='IO Module'
 and A.location_designation is not null

