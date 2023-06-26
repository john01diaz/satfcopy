-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW UT_VW_CABLE_CATALOGUE
AS
Select 
 A.database_name 
,A.object_identifier
,'111' as UT_ID
,CASE WHEN B.Object_Identifier is not null then 'Pass' else 'Fail' end as Test_Case
from Sigraph.Cable A
Left outer join SIgraph_Silver.S_CableCatalogue B On A.database_name=B.database_name and A.Object_Identifier=B.Object_Identifier

UNION

Select 
 A.database_name 
,A.object_identifier
,'110' as UT_ID
,CASE WHEN B.Object_Identifier is null then 'Pass' else 'Fail' end as Test_Case
from SIgraph_Silver.S_CableCatalogue A
Left outer join (
Select database_name,object_identifier from SIgraph_Silver.S_CableCatalogue Group by database_name,object_identifier Having Count(1)>1
) B On A.database_name=B.database_name and A.Object_Identifier=B.Object_Identifier

UNION

Select 
 A.database_name 
,A.object_identifier
,'112' as UT_ID
,CASE WHEN Size like '0.8mm2' then 'Fail' else 'Pass' end as Test_Case
from SIgraph_Silver.S_CableCatalogue A
Where A.EarthCore='TRUE';

DELETE FROM SIGRAPH_SILVER.UNIT_TEST_RESULTS WHERE Loader_Name=="CABLE_CATALOGUE";

INSERT INTO SIGRAPH_SILVER.UNIT_TEST_RESULTS (Loader_Name,database_name,Object_Identifier,UT_ID,Test_Case)
SELECT "CABLE_CATALOGUE" AS Loader_Name,
*
FROM UT_VW_CABLE_CATALOGUE