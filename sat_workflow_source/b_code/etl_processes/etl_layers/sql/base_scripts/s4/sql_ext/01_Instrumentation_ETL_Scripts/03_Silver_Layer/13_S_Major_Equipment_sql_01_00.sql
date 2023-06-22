
-- Extract the data using document class, as it will provide the mapping to area and areapath using layer class.
CREATE OR REPLACE TEMP VIEW VW_EquipmentExtract_From_DocumentClasses
As
Select 
Distinct 
A.database_name
,A.dynamic_class
,A.object_identifier
,PBS.Area_Code as Area
,Case when 
  Substring(Substring(DM_document_designation,
            locate("+",DM_document_designation)+1,100),1,
            locate("-",Substring(DM_document_designation,locate("+",DM_document_designation)+1,100))-1)
            <>'' 
      Then Substring(Substring(DM_document_designation,
            locate("+",DM_document_designation)+1,100),1,
            locate("-",Substring(DM_document_designation,locate("+",DM_document_designation)+1,100))-1)
      ELSE DM_document_designation END as EquipmentNo
,concat_ws('-',Site_Code,Coalesce(Revised_Plant_Code,Plant_Code),Coalesce(Revised_Process_unit,Process_Unit)) as AreaPath
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
INNER JOIN sigraph.Layer L ON L.database_name=B.database_name and L.name=Replace(B.name,' ','_')
INNER JOIN Sigraph_Reference.PlantBreakDown PBS ON 
PBS.Process_Unit=Case When L.Name like 'RAUM%' Then Substring(Replace(L.name,'RAUM_',''),1,100)
                      When Substring(L.name,1,charindex('_',L.Name )-1)=CS_loop_unit
                      Then Substring(L.name,charindex('_',L.Name )+1,100)
                      Else Substring(L.name,1,charindex('_',L.Name )-1) 
                  END
AND
PBS.Area_Code=regexp_extract(
      Case when 
      Substring(Substring(DM_document_designation,
            locate("+",DM_document_designation)+1,100),1,
            locate("-",Substring(DM_document_designation,locate("+",DM_document_designation)+1,100))-1)
            <>'' 
      Then Substring(Substring(DM_document_designation,
            locate("+",DM_document_designation)+1,100),1,
            locate("-",Substring(DM_document_designation,locate("+",DM_document_designation)+1,100))-1)
      ELSE DM_document_designation END,('^[0-9]+'),0
      )  


