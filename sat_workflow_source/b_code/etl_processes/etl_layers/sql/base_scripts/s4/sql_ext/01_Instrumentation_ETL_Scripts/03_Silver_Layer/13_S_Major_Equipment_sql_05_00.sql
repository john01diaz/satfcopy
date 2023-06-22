
CREATE OR REPLACE TEMP VIEW VW_Major_Equipments
As
Select
A.database_name
,A.Dynamic_Class
,A.Object_Identifier
,A.Area
,A.EquipmentNo as Parent_Equipment_No
,Case When A.EquipmentNo like '%_REP%' Then 'Service Switch / Reperaturschalter' 
      Else Coalesce(MME1.Description,MME2.Description,'_UDF_Undefined')
      END as Description
,Case When A.EquipmentNo like '%_REP%'                           Then 'Local Panel'
      When LENGTH(regexp_extract(A.EquipmentNo,'[A-Za-z]+',0))=3 Then 'Cabinet/Panel'
      Else Coalesce(MME1.EquipmentType,MME2.EquipmentType,'Junction Box')
      END as EquipmentType 
,'TRUE' as VendorSupplied
,'TRUE' as DwgRequired
,'TRUE' as Status
,translate(A.AreaPath,"[]","") as AreaPath
,VL.Type
,VL.Designation
,VL.Comment
,VL.Installation_site
,VL.Category
From (
Select * from VW_EquipmentExtract_From_DocumentClasses
UNION
Select * from VW_EquipmentExtract_From_ItemFunction
UNION
Select * from VW_MissingMajorEquipments
) as A
left outer join sigraph_Reference.Master_Major_Equipment_Type MME1 ON UPPER(MME1.Code)=UPPER(A.EquipmentNo)
left outer join sigraph_Reference.Master_Major_Equipment_Type MME2 ON UPPER(MME2.Code)=UPPER(regexp_extract(A.EquipmentNo,'[A-Za-z]+',0))
left outer join VW_Location VL ON VL.EquipmentNo=A.EquipmentNo
Where A.EquipmentNo is not null
Qualify Row_Number() Over(Partition by A.database_name,A.EquipmentNo order by A.Object_Identifier) =1

