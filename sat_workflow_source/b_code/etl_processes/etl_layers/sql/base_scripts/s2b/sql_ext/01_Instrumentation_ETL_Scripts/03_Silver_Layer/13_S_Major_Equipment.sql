

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
LEFT ANTI JOIN VW_EquipmentExtract_From_DocumentClasses B On A.Database_name=B.Database_name and A.EquipmentNo=B.EquipmentNo
Where A.RNT=1



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
) as L 


CREATE OR REPLACE TEMP VIEW VW_MissingMajorEquipments
As
Select Distinct
Con.database_name
,Con.dynamic_class
,Con.object_identifier
,'R' as Area
,Con.EquipmentNo
,'D015-MMP3-R' as AreaPath
From VW_Location as Con 
Left Anti Join (
Select Database_name,EquipmentNo from VW_EquipmentExtract_From_DocumentClasses
UNION
Select Database_name,EquipmentNo from VW_EquipmentExtract_From_ItemFunction
) as Cat ON Cat.Database_name=Con.Database_name and Cat.EquipmentNo=Con.EquipmentNo
Where Con.RNT=1

UNION
Select Distinct
Con.database_name
,Con.dynamic_class
,Con.object_identifier
,'R' as Area
,Con.Location_Designation as EquipmentNo
,'D015-MMP3-R' as AreaPath
From sigraph_Silver.S_ItemFunction as Con 
Left Anti Join (
Select Database_name,EquipmentNo from VW_EquipmentExtract_From_DocumentClasses
UNION
Select Database_name,EquipmentNo from VW_EquipmentExtract_From_ItemFunction
) as Cat ON Cat.Database_name=Con.Database_name and  Cat.EquipmentNo=Con.Location_Designation



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


 dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Major_Equipments", True)

 DF=spark.sql('Select * from VW_Major_Equipments where database_name in (Select * from VW_Database_names)')

 DF.write.save(
     path   = 'dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Major_Equipments'
     ,format = "delta"
     ,mode   = "overwrite"
     ,overwriteSchema = True
 )
