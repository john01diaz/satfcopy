
CREATE OR REPLACE TEMP VIEW VW_Union_All_Component
As
Select database_name
,object_identifier
,dynamic_class
,Parent_Equipment_No
,Equipment_No
,EquipmentType
,CatalogueNo
,Rack
,Item_dynamic_class
,Item_Object_identifier
,NumberOfEquipments
,Remarks
,Class
-- Below script added, as in Aveva we cannot have on same cabinet, dinrail/rack FTA and IO cards on same slot. It has to be a different slot or sequence.
--,Case When Row_Number() Over(Partition by parent_Equipment_No,Rack,Slot order by Item_Object_Identifier)>1
--      Then MAX(Coalesce(Cast(Slot as Bigint),0)) Over(Partition by parent_Equipment_No,Rack)
--      +Row_Number() Over(Partition by parent_Equipment_No,Rack order by Item_Object_Identifier)
--      Else Cast(Slot as Bigint)
--      END as Slot
From (
Select database_name,object_identifier,dynamic_class,Parent_Equipment_No,Equipment_No,EquipmentType,CatalogueNo,Rack
,Item_Object_identifier,Item_dynamic_class,Slot
,Count(1) Over(Partition by Parent_Equipment_No,EquipmentType) as NumberOfEquipments
,Case when Rack is not null then 'Rack' Else 'DinRail' END as Remarks 
,Class
from VW_Device_Extract
UNION
Select database_name,object_identifier,dynamic_class,Parent_Equipment_No,Equipment_No,EquipmentType,CatalogueNo,Rack,Item_Object_identifier,Item_dynamic_class,Slot
,Count(1) Over(Partition by Parent_Equipment_No,EquipmentType) as NumberOfEquipments
,Case when Rack is not null then 'Rack' Else 'DinRail' END as Remarks 
,Class
from VW_TerminalStrip
UNION
Select database_name,object_identifier,dynamic_class,Parent_Equipment_No,Equipment_No,EquipmentType,CatalogueNo,Rack ,Item_Object_identifier,Item_dynamic_class,Slot
,Count(1) Over(Partition by Parent_Equipment_No,EquipmentType) as NumberOfEquipments
,Case when Rack is not null then 'Rack' Else 'DinRail' END as Remarks 
,Class
from VW_IO_Module 
) as A

