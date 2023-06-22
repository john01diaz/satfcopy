CREATE OR REPLACE TEMP VIEW UT_VW_CABLE_SCHEDULE
AS 
Select 
 A.database_name
,A.object_identifier
,'147' as UT_ID
,CASE WHEN B.Object_Identifier is not null then 'Pass' else 'Fail' end as Test_Case
from Sigraph.Cable A
Left outer join sigraph_Silver.S_CableSchedule B 
On A.database_name=B.database_name 
and A.Object_Identifier=B.Object_Identifier

UNION

Select 
 A.database_name
,A.object_identifier
,'148' as UT_ID
,CASE WHEN B.Parent_Equipment_No is not null then 'Pass' else 'Fail' end as Test_Case
from sigraph_Silver.S_CableSchedule A
Left outer join sigraph_Silver.s_major_equipments B 
On A.database_name=B.database_name 
and A.From_Location=B.Parent_Equipment_No
Where A.From_Location is not null 

UNION

Select 
 A.database_name
,A.object_identifier
,'149' as UT_ID
,CASE WHEN B.Parent_Equipment_No is not null then 'Pass' else 'Fail' end as Test_Case
from sigraph_Silver.S_CableSchedule A
Left outer join sigraph_Silver.s_major_equipments B 
On A.database_name=B.database_name and A.To_Location=B.Parent_Equipment_No
Where A.To_Location is not null

UNION

Select 
 A.database_name
,A.object_identifier
,'150' as UT_ID
,CASE WHEN B.CatalogueNo is not null then 'Pass' else 'Fail' end as Test_Case
from sigraph_Silver.S_CableSchedule A
Left outer join sigraph_Silver.S_CableCatalogueNumber_Master B 
On A.database_name=B.database_name and A.CatalogueNo=B.CatalogueNo
Where A.Dynamic_Class<>'LC_Connection'


DELETE FROM SIGRAPH_SILVER.UNIT_TEST_RESULTS WHERE Loader_Name=="CABLE_SCHEDULE";

INSERT INTO SIGRAPH_SILVER.UNIT_TEST_RESULTS (Loader_Name,database_name,Object_Identifier,UT_ID,Test_Case)
SELECT "CABLE_SCHEDULE" AS Loader_Name,
*
FROM UT_VW_CABLE_SCHEDULE
