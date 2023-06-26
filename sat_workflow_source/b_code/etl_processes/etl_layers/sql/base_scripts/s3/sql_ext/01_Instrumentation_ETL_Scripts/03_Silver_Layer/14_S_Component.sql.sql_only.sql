
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
 Where A.Type in ('Device','FTA') and A.location_designation is not null;

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
 and location_designation is not NULL;

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
 and A.location_designation is not null;

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
) as A;

CREATE OR REPLACE TEMP VIEW VW_Component_With_DinRail
As
Select 
 A.database_name
,A.object_identifier
,A.dynamic_class
,A.Parent_Equipment_No
,A.Equipment_No
,A.EquipmentType
,A.CatalogueNo
,DinRail
,Remarks
,A.Class
,Item_dynamic_class
,Item_Object_identifier
From (
Select 
A.database_name
,A.object_identifier
,A.dynamic_class,
A.Parent_Equipment_No
,A.Equipment_No
,A.EquipmentType
,A.CatalogueNo
,A.Remarks
,A.Class
,Case -- IO DinRail 
     When A.EquipmentType='IO Module' and DeviceGroup=1 Then 'A'
     When A.EquipmentType='IO Module' and DeviceGroup=2 Then 'B'
     When A.EquipmentType='IO Module' and DeviceGroup=3 Then 'C'
     When A.EquipmentType='IO Module' and DeviceGroup=4 Then 'D'
     When A.EquipmentType='IO Module' and DeviceGroup=5 Then 'E'
     -- Device DinRail
     When A.EquipmentType='Device' and DeviceGroup=1 Then 'F'
     When A.EquipmentType='Device' and DeviceGroup=2 Then 'G'
     When A.EquipmentType='Device' and DeviceGroup=3 Then 'H'
     When A.EquipmentType='Device' and DeviceGroup=4 Then 'I'
     When A.EquipmentType='Device' and DeviceGroup=5 Then 'J'
     -- Terminal Strip DinRail
     When A.EquipmentType='Terminal Strip' and DeviceGroup=1 Then 'K'
     When A.EquipmentType='Terminal Strip' and DeviceGroup=2 Then 'L'
     When A.EquipmentType='Terminal Strip' and DeviceGroup=3 Then 'M'
     When A.EquipmentType='Terminal Strip' and DeviceGroup=4 Then 'N'
     When A.EquipmentType='Terminal Strip' and DeviceGroup=5 Then 'O'
     END as DinRail
,Item_dynamic_class
,Item_Object_identifier     
From (     
Select 
 A.database_name
,A.object_identifier
,A.dynamic_class
,A.Parent_Equipment_No
,A.Equipment_No
,A.EquipmentType
,A.CatalogueNo
,A.Remarks
,A.Class
,Case when NumberOfEquipments<=10 
      Then Ntile(1) Over(partition by A.Parent_Equipment_No,A.EquipmentType order by Item_Object_identifier)
      when NumberOfEquipments>=11 and NumberOfEquipments<=20 
      Then Ntile(2) Over(partition by A.Parent_Equipment_No,A.EquipmentType order by Item_Object_identifier)
      when NumberOfEquipments>=21 and NumberOfEquipments<=30 
      Then Ntile(3) Over(partition by A.Parent_Equipment_No,A.EquipmentType order by Item_Object_identifier)
      when NumberOfEquipments>=31 and NumberOfEquipments<=40 
      Then Ntile(4) Over(partition by A.Parent_Equipment_No,A.EquipmentType order by Item_Object_identifier)
      else Ntile(5) Over(partition by A.Parent_Equipment_No,A.EquipmentType order by Item_Object_identifier)
      END as DeviceGroup
,Item_dynamic_class
,Item_Object_identifier
From VW_Union_All_Component A
Where Rack is null 
) as A
) As A;

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
Where Rack is not null;

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