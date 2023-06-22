
-- The FTA and IO connections are system connections. hence these are not present in the Connection class.
-- The connection is derived by joining, loop number, io type, channel number with IO and FTA.
Create or Replace Temp view VW_Connection_Prep_Query_IO
AS
-- Possitive Connection
Select Distinct
IO.object_identifier
,IO.database_name
,'' as From_Pin_Dynamic_Class
,'' as From_Pin_Object_Identifier
,'' as To_Pin_Dynamic_Class
,'' as To_Pin_Object_Identifier
,IO.dynamic_Class as FROM_dynamic_Class
,IO.object_identifier as FROM_object_identifier
,IO.Item_dynamic_Class as FROM_Item_dynamic_Class
,IO.Item_object_identifier as FROM_Item_object_identifier
,FTA.dynamic_Class as To_dynamic_Class
,FTA.object_identifier as To_object_identifier
,FTA.Item_dynamic_Class as To_Item_dynamic_Class
,FTA.Item_object_identifier as To_Item_object_identifier
,'' as Cable_Object_Identifier
,'' as Wire_Object_Identifier
,IO.location_designation as From_Location
,IO.facility_designation as From_Facility
,IO.product_Key as From_Item
,Concat(IO.ChannelNumber,'+') as From_Terminal_Marking
,IO.Object_Identifier as Cable
,0 as Wire_Cross_Section
,1 as Wire_Markings
,FTA.location_designation as To_Location
,FTA.facility_designation as To_Facility 
,FTA.product_Key as To_Item
,Concat(FTA.ChannelNumber,'+') as To_Terminal_Marking
,Coalesce(IO.Loop_Number,FTA.Loop_Number) as Loop_Number
,Coalesce(IO.Tag_Number,FTA.Tag_Number) as Tag_Number
,Coalesce(IO.Document_Number,FTA.Document_Number) as Document_Number
from VW_IO_Card_To_FTA_Connection ConIO
Inner join sigraph_silver.S_ItemFunction IO 
ON ConIO.database_name=IO.Database_name
and ConIO.IO_Dynamic_Class=IO.Dynamic_Class
and ConIO.IO_Object_Identifier=IO.Object_Identifier
Inner join sigraph_silver.S_ItemFunction FTA
ON ConIO.database_name=IO.Database_name
and ConIO.FTA_Dynamic_Class=FTA.Dynamic_Class
and ConIO.FTA_Object_Identifier=FTA.Object_Identifier




