
Create or Replace Temp View VW_Terminals_Prep_4
As
select A.*
From (
Select
 A.database_name
,A.dynamic_class
,A.object_identifier
,A.Item_Dynamic_Class
,A.Item_object_Identifier
,Coalesce(A.Location_Designation,'') as Parent_Equipment_No
,A.Product_Key as Equipment_No
,From_Terminal_Marking as Marking
,A.Class
from sigraph_silver.S_Itemfunction A
Inner join sigraph_silver.S_Connection  B On A.database_name=B.database_name and A.dynamic_class=B.from_dynamic_class
and A.object_identifier=B.From_object_identifier
Where A.Type='FTA'
union
Select
 A.database_name
,A.dynamic_class
,A.object_identifier
,A.Item_Dynamic_Class
,A.Item_object_Identifier
,Coalesce(A.Location_Designation,'') as Parent_Equipment_No
,A.Product_Key as Equipment_No
,To_Terminal_Marking as Marking
,A.Class
from sigraph_silver.S_Itemfunction A
Inner join sigraph_silver.S_Connection  B On A.database_name=B.database_name and A.dynamic_class=B.To_dynamic_class
and A.object_identifier=B.To_object_identifier
) as A
left anti join (
Select * from VW_Terminals_Prep_1
UNION
Select * from VW_Terminals_Prep_2
UNION
Select * from VW_Terminals_Prep_3
) as B On A.database_name=B.database_name and A.dynamic_class=B.dynamic_class
and A.object_identifier=B.object_identifier and A.Marking=B.Marking



