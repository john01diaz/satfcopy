
CREATE OR REPLACE TEMP VIEW VW_Component_With_Rack
As

Select
 A.database_name
,A.object_identifier
,A.dynamic_class
,A.Parent_Equipment_No
,A.Equipment_No
,A.EquipmentType
,A.CatalogueNo
,A.Rack
,'Rack' as Remarks 
,A.Class
,Item_dynamic_class
,Item_Object_identifier
From VW_Union_All_Component as A 
Where Rack is not null



