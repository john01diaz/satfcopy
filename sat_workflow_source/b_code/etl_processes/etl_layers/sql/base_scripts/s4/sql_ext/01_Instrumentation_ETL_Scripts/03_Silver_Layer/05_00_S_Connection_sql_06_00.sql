
-- In Sigraph, the IO cards and FTA's are not connected using wire or cable. It is only symbolic representation.
-- Need to Generate the IO connection. The below query will join IO with FTAs
CREATE OR REPLACE TEMP VIEW VW_IO_Card_To_FTA_Connection_Query_1
As
-- With Item Slot property join
Select
IO.database_name
,IO.Object_identifier as IO_Object_identifier
,IO.Dynamic_Class as IO_Dynamic_Class
,FTA.ObjecT_Identifier as FTA_ObjecT_Identifier
,FTA.Dynamic_Class as FTA_Dynamic_Class
,IO.Loop_Number
from sigraph_silver.S_ItemFunction IO
Inner join sigraph_silver.S_ItemFunction FTA 
ON IO.database_name=FTA.database_name
and IO.Loop_Number=FTA.Loop_Number
and IO.IOType=FTA.IOType
and IO.Channelnumber=FTA.ChannelNumber
and IO.Item_Slot=FTA.Item_Slot
Where IO.Type='IO Module' and FTA.Type='FTA'


