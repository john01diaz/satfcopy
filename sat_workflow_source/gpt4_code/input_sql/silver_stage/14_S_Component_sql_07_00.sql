
Create OR Replace TEMP VIEW VW_Component_Extract
AS
Select 
 A.database_name
,A.dynamic_class
,A.object_identifier
,A.Parent_Equipment_No
,A.Equipment_No
,A.EquipmentType
,A.CatalogueNo
,A.DinRail
,A.Item_Object_identifier
,A.Item_dynamic_class
,Row_Number() Over(Partition by Parent_Equipment_No,DinRail order by Item_Object_identifier) as Sequence
,Remarks        
,A.Class
from 
(
Select * from VW_Component_With_DinRail
UNION 
Select * from VW_Component_With_rack
) as A
order by Parent_Equipment_No,EquipmentType,Remarks,DinRail,Sequence

