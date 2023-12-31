

CREATE OR REPLACE TEMP VIEW VW_IO_Card_To_FTA_Connection_Query_2
AS
-- Without Item Slot property join
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
LEFT ANTI JOIN VW_IO_Card_To_FTA_Connection_Query_1 CTE 
ON CTE.database_name=IO.database_name 
and CTE.IO_Dynamic_Class=IO.Dynamic_Class
and CTE.IO_Object_identifier=IO.Object_identifier
Where IO.Type='IO Module' and FTA.Type='FTA'


