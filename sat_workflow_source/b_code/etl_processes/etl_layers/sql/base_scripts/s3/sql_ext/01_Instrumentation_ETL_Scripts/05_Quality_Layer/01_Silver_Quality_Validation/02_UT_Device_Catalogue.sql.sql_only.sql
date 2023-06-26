-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW UT_VW_Device_Catalogue
as
Select 
 database_name
,object_identifier
,'107' as UT_ID
,CASE WHEN Coalesce(Type,'')<>'' then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_DeviceCatalogue 
Where Catalogue_RNT=1 and Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select 
database_name
,object_identifier
,'104' as UT_ID
,CASE WHEN Coalesce(Description,'')<>'' then 'Pass' else 'Fail' end as TestCase
from Sigraph_Silver.S_DeviceCatalogue
Where Catalogue_RNT=1 and Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select 
 database_name
,object_identifier
,'108' as UT_ID
,CASE WHEN Coalesce(Manufacturer,'')<>'' then 'Pass' else 'Fail' end as TestCase
from Sigraph_Silver.S_DeviceCatalogue
Where Catalogue_RNT=1 and Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select 
 database_name
,object_identifier
,'105' as UT_ID
,CASE WHEN Coalesce(ModelNo,'')<>'' then 'Pass' else 'Fail' end as TestCase
from Sigraph_Silver.S_DeviceCatalogue
Where Catalogue_RNT=1 and Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select 
 database_name
,object_identifier
,'109' as UT_ID
,CASE WHEN Row_Number() Over(Partition by ModelNo order by ModelNo)=1 then 'Pass' else 'Fail' end as TestCase
from Sigraph_Silver.S_DeviceCatalogue
Where Catalogue_RNT=1 and Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')



UNION

Select 
 database_name
,object_identifier
,'106' as UT_ID
,CASE WHEN Left<>0 and Left=(Length(Left_Marking)-Length(replace(Left_Marking,',','')))+1 And
           Right<>0 and Right=(Length(Right_Marking)-Length(replace(Right_Marking,',','')))+1
      Then 'Pass' 
      WHEN Left<>0 and Left=(Length(Left_Marking)-Length(replace(Left_Marking,',','')))+1 And
           Right=0
      Then 'Pass'
      WHEN Left=0 And
           Right<>0 and Right=(Length(Right_Marking)-Length(replace(Right_Marking,',','')))+1
      Then 'Pass'
      Else 'Fail' end as TestCase
from Sigraph_Silver.S_DeviceCatalogue
Where Catalogue_RNT=1 and Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION


Select 
 database_name
,Item_Object_Identifier as object_identifier
,'182' as UT_ID
,CASE WHEN row_number() Over(PARTITION BY A.database_name,A.Item_Object_Identifier,All_Marking order by All_Marking)=1 
Then 'Pass' else 'Fail' end as TestCase
From (
SELECT
*
,explode(Split(Marking,',')) as All_Marking
from (
Select Distinct
database_name
,Item_Object_Identifier
,Concat(Left_Marking,',',Right_Marking)as Marking
FROM sigraph_silver.s_devicecatalogue
) as A
) as A;

DELETE FROM SIGRAPH_SILVER.UNIT_TEST_RESULTS WHERE Loader_Name=="DEVICE_CATALOGUE";

INSERT INTO SIGRAPH_SILVER.UNIT_TEST_RESULTS (Loader_Name,database_name,Object_Identifier,UT_ID,Test_Case)
SELECT "DEVICE_CATALOGUE" AS Loader_Name,
*
FROM UT_VW_Device_Catalogue