
Create Or Replace Temp View VW_IO_Card_To_FTA_Connection_Query_3
As
Select
IO.database_name
,IO.Object_identifier as IO_Object_identifier
,IO.Dynamic_Class as IO_Dynamic_Class
,FTA.ObjecT_Identifier as FTA_ObjecT_Identifier
,FTA.Dynamic_Class as FTA_Dynamic_Class
,IO.Loop_Number
from (
Select *,Count(1) Over(Partition by IO.Database_name,IO.Loop_Number) as IO_Count
from sigraph_silver.S_ItemFunction IO
Where IO.Type='IO Module'
) as IO

Inner join sigraph_silver.S_ItemFunction FTA 
ON IO.database_name=FTA.database_name
and IO.Loop_Number=FTA.Loop_Number
and FTA.Type='FTA'
-- Ignore FTA and IO Connections which is identified using above query. Which provides more granularity properties to match. There are cases where we have IO and FTA connected with none of these properties matching. For these we have 1 IO and 1 FTA. So this query will join only based on loop and create the connection between the two.
LEFT ANTI JOIN VW_IO_Card_To_FTA_Connection_Query_1 IFT1 ON IFT1.Database_name=IO.database_name 
and IFT1.IO_Dynamic_Class=IO.Dynamic_Class
and IFT1.IO_Object_identifier=IO.Object_Identifier
LEFT ANTI JOIN VW_IO_Card_To_FTA_Connection_Query_2 IFT2 ON IFT2.Database_name=IO.database_name 
and IFT2.IO_Dynamic_Class=IO.Dynamic_Class
and IFT2.IO_Object_identifier=IO.Object_Identifier
Where IO.IO_Count=1 -- Make sure we have only one IO Card for given loop


