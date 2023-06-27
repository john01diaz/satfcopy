
-- The Equipments where we are not able to map it to the correct area for now, these will be mapped to Aveva Default Area, later discipline will review the same and correct the data in Aveva. - Confirmed by Jakob/Zahra - 11-8-2022
CREATE OR REPLACE TEMP VIEW VW_MissingMajorEquipments
As
-- The Equipments present in master table but not present in transactional table.
Select Distinct
Con.database_name
,Con.dynamic_class
,Con.object_identifier
,'Default' as Area
,Con.EquipmentNo
,'' as AreaPath
From VW_Location as Con 
Left Anti Join (
Select Database_name,EquipmentNo from VW_EquipmentExtract_From_DocumentClasses
UNION
Select Database_name,EquipmentNo from VW_EquipmentExtract_From_ItemFunction
) as Cat ON Cat.Database_name=Con.Database_name and Cat.EquipmentNo=Con.EquipmentNo
Where Con.RNT=1

-- The terminal strips present in field and they dont have any junction box defined. In this case, the terminal strip tag will be considered as junction box.
UNION
Select Distinct
Con.database_name
,Con.dynamic_class
,Con.object_identifier
,'Default' as Area
,Con.Location_Designation as EquipmentNo
,'' as AreaPath
From sigraph_Silver.S_ItemFunction as Con 
Left Anti Join (
Select Database_name,EquipmentNo from VW_EquipmentExtract_From_DocumentClasses
UNION
Select Database_name,EquipmentNo from VW_EquipmentExtract_From_ItemFunction
) as Cat ON Cat.Database_name=Con.Database_name and  Cat.EquipmentNo=Con.Location_Designation


