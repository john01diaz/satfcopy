
-- Get only Instrument data by using below query.
create or replace temp view IO_Allocations_Instrument
as
Select Distinct
 SR.database_name
,SR.object_identifier
,SR.Tag_Number
,SR.Parent_Equipment_No
,SR.IOType
,SR.Equipment_No
,SR.CatalogueNo
,SR.ChannelNumber
,SR.Class
From IO_Allocations_Prep_Query SR
-- Get only Instrument data by using below query.
LEFT SEMI JOIN Sigraph_silver.S_ItemFunction DP 
ON DP.database_name=SR.database_name 
and DP.Tag_Number=SR.Tag_Number
and DP.Type='Field Device'
Where SR.Tag_Number is not null

