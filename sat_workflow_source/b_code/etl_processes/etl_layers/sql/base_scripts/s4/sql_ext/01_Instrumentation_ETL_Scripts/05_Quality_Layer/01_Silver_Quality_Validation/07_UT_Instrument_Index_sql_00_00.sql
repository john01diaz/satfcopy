-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW UT_VW_INSTRUMENT_INDEX
AS
--Select Distinct
-- A.database_name
--,A.object_identifier
--,'130' as UT_ID
--,CASE WHEN B.Object_Identifier is not null then 'Pass' else 'Fail' end as Test_Case
--from sigraph_Silver.S_Itemfunction A
--Left outer join Sigraph_Silver.S_Instrument_index B On A.database_name=B.database_name 
--and A.Object_Identifier=B.Object_Identifier
--Where A.Type='Field Device'

--UNION

Select 
 A.database_name
,A.object_identifier
,'131' as UT_ID
,CASE WHEN Row_Number() over(Partition By database_name,Object_identifier, Coalesce(Junction_Box,'') ,A.TagNo
                      order by Object_Identifier)=1
      Then 'Pass' 
      Else 'Fail' End as Test_Case
from Sigraph_Silver.S_Instrument_index A
Where A.Class='Instrumentation'

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'132' as UT_ID
,CASE WHEN A.Data_Type='Soft tag' and A.Area='Default' Then 'Pass'
      WHEN B.Area is not null then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Instrument_index A
left outer join Sigraph_reference.PlantBreakdown B ON A.Area=B.Area
Where A.Class='Instrumentation'

UNION

Select Distinct
 database_name
,object_identifier
,'133' as UT_ID
,CASE WHEN FormatName in (
 'RHLND Instrument Tag 1'
,'RHLND Instrument Tag 2'
,'RHLND Instrument Tag 3'
,'RHLND Instrument Tag 4'
,'RHLND Instrument Tag 5'
,'RHLND Instrument Tag 6'
,'RHLND Instrument Tag 8'
,'RHLND Instrument Tag 9'
,'RHLND Free Form'
)   Then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Instrument_index A
Where A.Class='Instrumentation'

UNION

Select Distinct
 database_name
,object_identifier
,'134' as UT_ID
,CASE WHEN B.Site_Code is not null  Then 'Pass' 
      When A.Area='Default' then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Instrument_index A
left outer join Sigraph_reference.PlantBreakdown B 
ON A.AreaPath=concat_ws('-',Site_Code,Coalesce(Engineering_Plant_Code,Plant_Code),Coalesce(Engineering_Process_Unit,Process_Unit))
Where A.Class='Instrumentation'

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'135' as UT_ID
,CASE WHEN B.Catalogue_Name is not null  then 'Pass' else 'Fail' end as TestCase
from Sigraph_Silver.S_Instrument_index A
left outer join Sigraph_Silver.s_field_device_catalogue B 
ON A.Wiring_Config=B.Catalogue_Name 
Where Coalesce(A.Wiring_Config,'')<>''
and  A.Class='Instrumentation'

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'136' as UT_ID
,CASE WHEN SpecialRemarks= 'Mechanical Connection. Need to be added in the drawing' OR Data_type='Soft tag'
      Then 'Pass' else 'Fail' end as TestCase
from Sigraph_Silver.S_Instrument_index A
Where Coalesce(A.Wiring_Config,'')=''
and A.Class='Instrumentation'

