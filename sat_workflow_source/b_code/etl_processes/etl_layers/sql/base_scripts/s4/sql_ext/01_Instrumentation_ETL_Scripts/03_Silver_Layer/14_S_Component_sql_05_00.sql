
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
) As A

