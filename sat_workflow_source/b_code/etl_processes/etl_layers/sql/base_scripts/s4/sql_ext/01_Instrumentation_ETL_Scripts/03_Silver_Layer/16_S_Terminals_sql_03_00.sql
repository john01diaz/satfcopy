
-- IO Terminal markings
Create Or Replace Temp View VW_Terminals_Prep_3
As
Select
 A.database_name
,A.dynamic_class
,A.object_identifier
,A.Item_Dynamic_Class
,A.Item_object_Identifier
,Coalesce(A.Location_Designation,'') as Parent_Equipment_No
,A.Product_Key as Equipment_No
,B.TerminalsPerMarking as Marking
,A.Class
from sigraph_silver.S_Itemfunction A
Inner join sigraph_silver.S_IO_Catalogue B On A.database_name=B.database_name and A.dynamic_class=B.dynamic_class
and A.object_identifier=B.object_identifier


