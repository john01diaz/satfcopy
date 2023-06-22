
-- Get only Devices data by using below query.
--create or replace temp view IO_Allocations_Instrument
--as
Select Distinct
 SR.*
From IO_Allocations_Prep_Query SR
-- Get only Instrument data by using below query.
LEFT ANTI JOIN Sigraph_silver.S_ItemFunction DP 
ON DP.database_name=SR.database_name 
and DP.Tag_Number=SR.Tag_Number
and DP.Type='Field Device'
Where SR.Loop_Number='0313H4960'

