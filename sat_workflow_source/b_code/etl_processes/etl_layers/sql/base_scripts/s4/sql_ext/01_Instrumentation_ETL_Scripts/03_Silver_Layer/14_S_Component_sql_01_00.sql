
-- MAGIC %sql
 CREATE OR Replace TEMP VIEW VW_Device_Extract
 AS
 Select Distinct
  A.database_name
 ,A.object_identifier
 ,A.dynamic_class
 ,A.Location_designation as Parent_Equipment_No
 ,A.Product_key as Equipment_No
 ,'Device' as EquipmentType
 ,B.ModelNo as CatalogueNo
 ,A.Rack
 ,A.Item_Object_identifier
 ,A.Item_dynamic_class
 ,Case When Coalesce(A.Item_Slot,'')<>'' and Coalesce(A.Item_Slot,'')<>'0' then Item_Slot 
       END as Slot
 ,A.Class
 from sigraph_silver.S_Itemfunction A
 Inner join sigraph_silver.S_Item_Function_Model B 
 On A.database_name=B.database_name
 and A.Item_Dynamic_Class=B.Item_Dynamic_Class
 and A.Item_Object_identifier=B.Item_Object_identifier
 and A.Dynamic_Class=B.Dynamic_Class
 and A.Object_Identifier=B.Object_Identifier
 Where A.Type in ('Device','FTA') and A.location_designation is not null

