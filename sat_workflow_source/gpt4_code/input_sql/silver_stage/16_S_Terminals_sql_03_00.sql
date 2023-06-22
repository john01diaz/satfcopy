
Create Or Replace Temp View VW_Terminals_Prep_3
As
-- Get the terminals for IO, by using Channel Number + and -
Select
 A.database_name
,A.dynamic_class
,A.object_identifier
,A.Item_Dynamic_Class
,A.Item_object_Identifier
,Coalesce(A.Location_Designation,'') as Parent_Equipment_No
,A.Product_Key as Equipment_No
,Concat(A.ChannelNumber,'+')  as Marking
,A.Class
from sigraph_silver.S_Itemfunction A
Where Type in ('IO Module','FTA') and A.ChannelNumber is not null and A.ChannelNumber<>'' and A.ChannelNumber<>'0'
and Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1
UNION
-- Get the terminals for IO, by using Channel Number + and -

Select 
 A.database_name
,A.dynamic_class
,A.object_identifier
,A.Item_Dynamic_Class
,A.Item_object_Identifier
,Coalesce(A.Location_Designation,'') as Parent_Equipment_No
,A.Product_Key as Equipment_No
,Concat(A.ChannelNumber,'-')  as Marking
,A.Class
from sigraph_silver.S_Itemfunction A
Where Type in ('IO Module') and A.ChannelNumber is not null and A.ChannelNumber<>'' and A.ChannelNumber<>'0'
and Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1


