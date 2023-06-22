
-- Extract the cable schedule using connection class, where we have cable details
-- The connections where we  have cables will be extracted in next view
CREATE OR REPLACE TEMP VIEW VW_Cable_Extract
AS
Select Distinct
 C.Database_name
,C.dynamic_class
,C.Object_Identifier
,C.LC_item_product_des as CableNumber
,Con.From_Location
,Con.To_Location
,CLM.CatalogueNo as CatalogueNo
,Cast(LC_cable_length as Decimal(18,2)) as Length
,Case When UPPER(CS_loop_status) like '%PLAN%' Then 'Being Planned' 
      When UPPER(CS_loop_status) like '%INA%' Then 'Remove'
      Else 'As Built'
      END as ProjectStatus
,CS_loop_status_remark as Remarks
,'' as Gland_From
,'' as Gland_To
,'' as Adapter_From
,'' as Adapter_To	
,CL.Class as Discipline
-- Additional properties
,CL.size as Cross_section
,CL.Class

,C.LC_cable_drum_number As Cable_Drum_Number
,CL.Colour1  As Color
,C.LC_installation_vertices As Laying_Corner_Point
,C.LC_Item_ext_document As External_document_of_item
,C.LC_installed_cable_length As Installed_Length
,C.LC_installation_date As Installation_Date
,C.LC_cable_length As Estimated_Length
,C.LC_item_function_text_1 As Function_text_1
,C.LC_installation_level As Level_of_Installation
-- Picklist Property
,get_loop_picklist_display_value(array('LC_range',C.LC_range)) as `Range`
from sigraph.Cable C 
Inner join sigraph_Silver.S_CableCatalogue as CL ON C.database_name=CL.database_name and C.Object_Identifier=CL.Object_Identifier
Inner join sigraph_Silver.S_CableCatalogueNumber_Master as CLM ON C.database_name=CLM.database_name and C.Object_Identifier=CLM.Cable_Object_Identifier
Left outer join sigraph_Silver.S_Connection Con ON Con.database_name=C.database_name and Con.Cable=C.LC_item_product_des
-- Loop Mapping
Left outer join (
Select Distinct 
Con.database_name
,Con.Cable_Object_Identifier
,l.CS_loop_status
,l.CS_loop_status_remark
From (
      Select * From sigraph_Silver.S_Connection 
      qualify Row_Number() Over(Partition by database_name,Cable_Object_Identifier order by object_identifier)=1
     )Con
Inner join sigraph.loop l 
on l.loop_database_name=Con.database_name 
and l.CS_loop_id=Con.Loop_Number
) as LP 
ON LP.database_name=C.database_name 
and LP.Cable_Object_Identifier=C.object_identifier

