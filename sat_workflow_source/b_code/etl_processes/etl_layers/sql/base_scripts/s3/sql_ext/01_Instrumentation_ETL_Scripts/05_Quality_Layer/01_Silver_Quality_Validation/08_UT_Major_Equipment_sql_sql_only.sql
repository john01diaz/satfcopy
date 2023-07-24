-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW UT_VW_MAJOR_EQUIPMENTS
AS
Select Distinct
A.database_name
,A.object_identifier
,'137' as UT_ID
,CASE WHEN B.Object_Identifier is not null then 'Pass' else 'Fail' end as Test_Case
from (
Select ObjecT_Identifier,database_name,Location_Designation
,Row_Number() Over(Partition by database_name,Location_Designation order by Object_Identifier) as RNT
From Sigraph_Silver.S_ItemFunction
Where Location_Designation is not null
) as A
Left outer join sigraph_Silver.s_major_equipments B 
On A.database_name=B.database_name and A.Location_Designation=B.Parent_Equipment_No 
Where A.RNT=1

UNION

Select Distinct
database_name
,object_identifier
,'138' as UT_ID
,CASE WHEN Row_Number() Over(Partition by areapath,area,Parent_Equipment_No order by Parent_Equipment_No)=1 then 'Pass' else 'Fail' end as Test_Case
from  sigraph_Silver.s_major_equipments

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'139' as UT_ID
,CASE When B.Site_code is not null  then 'Pass' 
      When A.Area='Default' Then 'Pass'
      Else 'Fail' end as Test_Case
from sigraph_Silver.s_major_equipments A
left outer join Sigraph_reference.PlantBreakdown B ON A.Area=B.area

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'140' as UT_ID
,CASE When B.Site_Code is not null  then 'Pass' 
      When A.Area='Default' Then 'Pass'
      Else 'Fail' end as TestCase
from sigraph_Silver.s_major_equipments A
left outer join Sigraph_reference.PlantBreakdown B 
ON A.AreaPath=Concat(B.Site_Code,"-",Coalesce(Engineering_Plant_Code,B.Plant_Code),"-"
                     ,Coalesce(Engineering_Process_Unit,B.Process_Unit))

UNION

Select Distinct
 database_name
,object_identifier
,'141' as UT_ID
,CASE When EquipmentType is not null  then 'Pass' Else 'Fail' end as TestCase
from sigraph_Silver.s_major_equipments A;

DELETE FROM SIGRAPH_SILVER.UNIT_TEST_RESULTS WHERE Loader_Name=="MAJOR_EQUIPMENTS";

INSERT INTO SIGRAPH_SILVER.UNIT_TEST_RESULTS (Loader_Name,database_name,Object_Identifier,UT_ID,Test_Case)
SELECT "MAJOR_EQUIPMENTS" AS Loader_Name,
*
FROM UT_VW_MAJOR_EQUIPMENTS