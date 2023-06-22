-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW UT_VW_LOOP_INDEX
AS
Select 
 B.loop_database_name as database_name 
,B.loop_object_identifier as object_identifier
,'124' as UT_ID
,CASE WHEN B.loop_Object_Identifier is not null then 'Pass' else 'Fail' end as Test_Case
From  Sigraph_Silver.S_Loop_Index A
Left outer join Sigraph.Loop B 
ON A.database_name=B.loop_database_name 
and A.dynamic_class=B.loop_dynamic_class 
and A.Object_identifier=B.loop_Object_Identifier
Where A.Class='Instrumentation'

UNION

Select 
 A.database_name as database_name 
,A.object_identifier as object_identifier
,'125' as UT_ID
,CASE WHEN Count(1) Over(Partition by database_name,dynamic_class,Object_identifier order by object_identifier)=1 
      Then 'Pass' else 'Fail' end as Test_Case    
From  Sigraph_Silver.S_Loop_Index A
Where A.Class='Instrumentation'

UNION

Select Distinct
 A.database_name as database_name 
,A.object_identifier as object_identifier
,'126' as UT_ID
,CASE WHEN B.Area_Code is not null then 'Pass' else 'Fail' end as Test_Case
From  Sigraph_Silver.S_Loop_Index A
left outer join sigraph_reference.PlantbreakdownStructure B 
ON A.Area=B.Area_Code
Where A.Class='Instrumentation'

UNION

Select Distinct
 A.database_name as database_name 
,A.object_identifier as object_identifier
,'127' as UT_ID
,CASE WHEN B.site_code is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Loop_Index A
left outer join sigraph_reference.PlantbreakdownStructure B 
ON A.AreaPath=Concat(B.site_code,"-",B.Plant_Code,"-",B.Process_Unit)
Where A.Class='Instrumentation'

UNION

Select Distinct
 A.database_name as database_name 
,A.object_identifier as object_identifier
,'128' as UT_ID
,CASE WHEN UPPER(Status) in ('AS BUILT','BEING PLANNED','DESIGN PHASE','DISMOUNTED','HOLD!','INACTIVE','MOUNTING','OUT OF ORDER','TO BE DELETED')  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Loop_Index A
Where A.Class='Instrumentation'

UNION

Select Distinct
 A.database_name as database_name 
,A.object_identifier as object_identifier
,'129' as UT_ID
,CASE WHEN FormatName in (
'RHLND Loop Tag 1'
,'RHLND Loop Tag 2'
,'RHLND Loop Tag 3'
,'RHLND Loop Tag 4'
,'RHLND Loop Tag 5'
,'RHLND Loop Tag 6'
,'RHLND Loop Tag 7'
,'RHLND Loop Tag 9'
,'RHLND Loop Tag 11') 
Then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Loop_Index A
Where A.Class='Instrumentation'

