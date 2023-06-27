
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
      = PBS.Area


