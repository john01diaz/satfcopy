
-- MAGIC %sql
 CREATE OR Replace TEMP VIEW VW_TerminalStrip
 AS
 Select Distinct
  TM.database_name
 ,TM.object_identifier
 ,TM.dynamic_class
 ,TM.location_designation as Parent_Equipment_No
 ,TM.product_key as Equipment_No
 ,'Terminal Strip' as EquipmentType
 ,null as CatalogueNo
 ,TM.Rack
 ,TM.Item_Object_identifier
 ,TM.Item_dynamic_class
 ,null as Slot
 ,TM.Class
 From sigraph_silver.S_ItemFunction TM
 Where Type='Terminal Strip'
 and location_designation is not NULL

