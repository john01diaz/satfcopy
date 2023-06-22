
CREATE OR REPLACE TEMP VIEW VW_Connection_Prep_Query_Non_IO
As
Select Distinct
Con.object_identifier
,Con.database_name
,RPN.Dynamic_Class as From_Pin_Dynamic_Class
,RPN.Object_Identifier as From_Pin_Object_Identifier
,LPN.Dynamic_Class as To_Pin_Dynamic_Class
,LPN.Object_Identifier as To_Pin_Object_Identifier
,FR.dynamic_Class as FROM_dynamic_Class
,FR.object_identifier as FROM_object_identifier
,FR.Item_dynamic_Class as FROM_Item_dynamic_Class
,FR.Item_object_identifier as FROM_Item_object_identifier
,To.dynamic_Class as To_dynamic_Class
,To.object_identifier as To_object_identifier
,To.Item_dynamic_Class as To_Item_dynamic_Class
,To.Item_object_identifier as To_Item_object_identifier
,C.Object_Identifier as Cable_Object_Identifier
,WF.Object_Identifier as Wire_Object_Identifier
,FR.location_designation as From_Location
,FR.facility_designation as From_Facility
,FR.product_Key as From_Item
,RPN.Terminal_Marking as From_Terminal_Marking
,C.LC_item_product_des as Cable
,WF.LC_wire_function_x_section as Wire_Cross_Section
,DCC.Core_Markings as Wire_Markings
,To.location_designation as To_Location
,To.facility_designation as To_Facility 
,To.product_Key as To_Item
,LPN.Terminal_Marking as To_Terminal_Marking
,Coalesce(FR.Loop_Number,To.Loop_Number) as Loop_Number
,Coalesce(FR.Tag_Number,To.Tag_Number) as Tag_Number
,Coalesce(FR.Document_Number,To.Document_Number) as Document_Number

From (
Select * from Vw_Connection_Pin_Extract_1
UNION
Select * from Vw_Connection_Pin_Extract_2
) Con
-- Left 
Left outer join sigraph_silver.S_Pin RPN ON RPN.database_name=Con.database_name 
and RPN.Dynamic_Class=Con.From_Pin_Dynamic_Class
and RPN.Object_Identifier=Con.From_Pin_Object_Identifier 

Left outer join sigraph_silver.S_ItemFunction as Fr 
On FR.database_name=RPN.database_name 
and FR.Dynamic_Class=RPN.Function_Dynamic_Class
and FR.Object_Identifier=RPN.Function_Object_Identifier

-- Right 
Left outer join sigraph_silver.S_Pin LPN ON LPN.database_name=Con.database_name 
and LPN.Dynamic_Class=Con.To_Pin_Dynamic_Class
and LPN.Object_Identifier=Con.To_Pin_Object_Identifier 

LEFT OUTER JOIN sigraph_silver.S_ItemFunction as To 
On To.database_name=LPN.database_name 
and To.Dynamic_Class=LPN.Function_Dynamic_Class
and To.Object_Identifier=LPN.Function_Object_Identifier
-- Wire and Cable
LEFT OUTER join sigraph.Wire_function WF 
ON WF.database_name=Con.database_name 
and WF.LC_wire_connection_rel_href = Con.object_identifier

LEFT OUTER join sigraph.Cable C 
ON WF.database_name=C.database_name 
and WF.LC_item_functions_rel_href = C.object_identifier

Left outer join sigraph_silver.S_CableCoreCatalogue DCC 
ON DCC.database_name=WF.database_name 
and DCC.object_Identifier=WF.Object_identifier


