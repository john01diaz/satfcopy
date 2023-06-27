
-- Extract the data using document class, as it will provide the mapping to area and areapath using layer class.
CREATE OR REPLACE TEMP VIEW VW_EquipmentExtract_From_DocumentClasses
As
Select
A.database_name
,A.dynamic_class
,A.object_identifier
,PBS.Area
,A.EquipmentNo
,concat_ws('-',PBS.Site_Code,Coalesce(PBS.Engineering_Plant_Code,PBS.Plant_Code)
            ,Coalesce(PBS.Engineering_Process_unit,PBS.Process_Unit)) as AreaPath
From (
Select 
Distinct 
A.database_name
,A.dynamic_class
,A.object_identifier
--EquipmentNo
,Case when 
  Substring(Substring(DM_document_designation,
            locate("+",DM_document_designation)+1,100),1,
            locate("-",Substring(DM_document_designation,locate("+",DM_document_designation)+1,100))-1)
            <>'' 
      Then Substring(Substring(DM_document_designation,
            locate("+",DM_document_designation)+1,100),1,
            locate("-",Substring(DM_document_designation,locate("+",DM_document_designation)+1,100))-1)
      ELSE DM_document_designation END as EquipmentNo
--Plant_Code
,Coalesce(Replace(Replace(Replace(Replace(L.CS_production_unit,'-',''),'MMP4','MMP64'),'MMP6','MMP64'),'MMP644','MMP64'),'') as Plant_Code      
-- Process_unit
,Replace(TRIM(
  Case When substring(L.name,1,Charindex('_',L.name)-1)='RAUM' Then substring(L.name,Charindex('_',L.name)+1,200) 
       When L.name like 'OFEN%' Then L.name Else substring(L.name,Charindex('_',L.name)+1,200) 
  End),' ','_') as Process_unit
-- Area Code
,regexp_extract(
      Case when 
      Substring(Substring(DM_document_designation,
            locate("+",DM_document_designation)+1,100),1,
            locate("-",Substring(DM_document_designation,locate("+",DM_document_designation)+1,100))-1)
            <>'' 
      Then Substring(Substring(DM_document_designation,
            locate("+",DM_document_designation)+1,100),1,
            locate("-",Substring(DM_document_designation,locate("+",DM_document_designation)+1,100))-1)
      ELSE DM_document_designation END,('^[0-9]+'),0
      )   as Area
FROM (
Select database_name,dynamic_class,object_identifier,DM_Folder_node_DM_Document_dyn_class,DM_Folder_node_DM_Document_href 
from sigraph.DM_Terminal_diagram
UNION
Select database_name,dynamic_class,object_identifier,DM_Folder_node_DM_Document_dyn_class,DM_Folder_node_DM_Document_href
from sigraph.DM_Circuit_diagram
UNION
Select database_name,dynamic_class,object_identifier,DM_Folder_node_DM_Document_dyn_class,DM_Folder_node_DM_Document_href
from sigraph.DM_Cabinet_diagram
) as A
Inner join sigraph.DM_Document_id D 
On A.database_name=D.database_name 
and  D.DM_Document_DM_Document_id_dyn_class=A.dynamic_class 
and  D.DM_Document_DM_Document_id_href=A.object_identifier
and DM_document_kind_class='EMV'
-- Folder
Inner join (
Select Distinct database_name,object_identifier,dynamic_class,Pv_base_element_name as name 
FROM sigraph.DM_Folder_node_1
UNION
Select Distinct database_name,object_identifier,dynamic_class,Pv_base_element_name as name 
FROM sigraph.DM_Folder_node_2
UNION
Select Distinct database_name,object_identifier,dynamic_class,Pv_base_element_name as name 
FROM sigraph.DM_Folder_node_3
UNION
Select Distinct database_name,object_identifier,dynamic_class,Pv_base_element_name as name 
FROM sigraph.DM_Folder_node_4
) as B 
On A.database_name=B.database_name 
and A.DM_Folder_node_DM_Document_dyn_class=B.dynamic_class 
and A.DM_Folder_node_DM_Document_href=B.object_identifier
-- Join with Layer
INNER JOIN sigraph.Layer L 
ON L.database_name=B.database_name 
and ( (L.name=Replace(B.name,' ','_'))
      or (replace(replace(replace(replace(replace(replace(regexp_extract(upper(trim(B.name)),r"[\[|\(](.*?)[\]|\)]\s(.*)", 0),"[",""),"]","")," ","_"),"-",""),")",""),"(","")== L.name))
) as A
Inner JOIN Sigraph_Reference.PlantBreakDown PBS 
ON    A.Plant_Code = PBS.Plant_Code
AND   A.Process_Unit = PBS.Process_Unit
AND   case when length(A.Area)==2 then concat('00',A.Area) when length(A.Area)==3 then concat('0',A.Area)  else A.Area end    
      = PBS.Area;

-- If all equipments are not able to extract usign Documents logic, use below logic, which will map the equipment to an instrument and get the Area and Areapath of an instrument and associate that to equipment.

Create OR REPLACE TEMP VIEW VW_EquipmentExtract_From_ItemFunction
AS
Select 
A.database_name
,A.Dynamic_Class
,A.Object_Identifier
,A.Area
,A.EquipmentNo
,A.AreaPath
From (
Select Distinct
l.Area
,A.Location_Designation as EquipmentNo
,l.AreaPath
,A.database_name
,A.Dynamic_Class
,A.Object_Identifier
,Row_Number() Over(Partition by A.Database_name,A.Location_Designation order by A.Loop_Number) as RNT
from sigraph_Silver.S_ItemFunction A
Inner join sigraph_Silver.S_Instrument_Index l 
on a.database_name=l.database_name 
and A.loop_element_dynamic_Class=l.Dynamic_Class 
and A.Loop_Element_Object_Identifier=l.Object_Identifier
Where A.Location_Designation is not null 
) as A 
-- Extract only those equipments, which are not present in document class
LEFT ANTI JOIN VW_EquipmentExtract_From_DocumentClasses B On A.Database_name=B.Database_name and A.EquipmentNo=B.EquipmentNo
Where A.RNT=1;

-- Get all the Major Equipments from Main_Location and Sub_Location

CREATE OR REPLACE TEMP VIEW VW_Location
AS
Select 
*
,Row_Number() Over(Partition by Database_name,EquipmentNo order by L.object_identifier) as RNT
From (
Select 
S.database_name
,S.dynamic_class
,S.object_identifier
,S.LC_location_key as EquipmentNo
,Null As Type
,Null As Designation
,Null As Comment
,Null As Installation_site
,Null As Category
from sigraph.Sub_location S
UNION
Select 
S.database_name
,S.dynamic_class
,S.object_identifier
,S.LC_location_key as EquipmentNo
,S.LC_main_location_type As Type
,S.LC_location_description As Designation
,S.LC_location_comment As Comment
,S.LC_main_location_site_dummy As Installation_site
,S.LC_main_location_type_dummy As Category
from sigraph.Main_location S
) as L;

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
) as Cat ON Cat.Database_name=Con.Database_name and  Cat.EquipmentNo=Con.Location_Designation;

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