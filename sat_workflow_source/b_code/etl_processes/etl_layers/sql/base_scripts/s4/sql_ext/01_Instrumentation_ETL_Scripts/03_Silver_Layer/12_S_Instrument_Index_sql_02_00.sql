
-- Soft tags data loading script
CREATE OR REPLACE TEMP VIEW VW_SoftTag_Instrument_List
AS 
Select  Distinct
A.database_name
,A.Dynamic_Class
,A.object_identifier
,'VINS' as ClassName
,'As Built' as Status
,'Default' as Area
,'' as AreaPath
,'Instrumentation' as Class
,Tag_Number as TagNo
,'RHLND Free Form' as FormatName
,'Soft tag' as Device_Type
,'Soft tag' as OperatingPrinc
,'FLD' as ISALocation
,Junction_Box
,'Soft tag' as SpecialRemarks
from sigraph_silver.S_IO_Allocations  A
Where IsSoftTag=1

