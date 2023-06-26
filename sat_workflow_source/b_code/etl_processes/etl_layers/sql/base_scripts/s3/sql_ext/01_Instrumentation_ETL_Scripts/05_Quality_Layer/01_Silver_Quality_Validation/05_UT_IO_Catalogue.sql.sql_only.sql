-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW UT_VW_IO_CATALOGUE
AS
Select 
 database_name
,object_identifier
,"122" as UT_ID
,CASE WHEN Coalesce(Model_Number,'')<>'' then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_IO_Catalogue
Where Catalogue_RNT=1

UNION

Select 
 database_name
,object_identifier
,"123" as UT_ID
,CASE WHEN Row_Number() Over(Partition by Model_Number,IOType,TerminalsPerMarking order by Model_Number)=1 then 'Pass' else 'Fail' end as TestCase
from Sigraph_Silver.S_IO_Catalogue 
Where Catalogue_RNT=1;

DELETE FROM SIGRAPH_SILVER.UNIT_TEST_RESULTS WHERE Loader_Name=="IO_CATALOGUE";

INSERT INTO SIGRAPH_SILVER.UNIT_TEST_RESULTS (Loader_Name,database_name,Object_Identifier,UT_ID,Test_Case)
SELECT "IO_CATALOGUE" AS Loader_Name,
*
FROM UT_VW_IO_CATALOGUE