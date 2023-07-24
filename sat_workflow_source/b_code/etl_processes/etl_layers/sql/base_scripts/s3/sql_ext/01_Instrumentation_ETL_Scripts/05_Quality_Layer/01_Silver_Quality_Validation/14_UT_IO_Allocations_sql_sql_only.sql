-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW UT_VW_IO_ALLOCATIONS
AS
Select Distinct
 A.database_name
,A.object_identifier
,'172' as UT_ID
,CASE WHEN B.Object_Identifier is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_silver.S_ItemFunction A
Inner join sigraph_silver.s_io_catalogue IOC ON IOC.database_name=A.database_name 
and IOC.object_identifier=A.Object_identifier
left outer join Sigraph_Silver.S_IO_Allocations B 
ON A.database_name=B.database_name 
and A.Object_Identifier=B.Object_Identifier
Where  Type='IO Module' and A.Loop_Number is not null
and A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 database_name
,object_identifier
,'173' as UT_ID
,CASE WHEN Row_Number() Over(Partition by Database_name, Object_Identifier,Tag_Number Order by Object_Identifier)=1
      Then 'Pass' else 'Fail' end as Test_Case
from 
(Select Distinct
Tag_Number
,Parent_Equipment_No
,IOType
,Equipment_No
,CatalogueNo
,ChannelNumber
,database_name
,object_identifier
From Sigraph_Silver.S_IO_Allocations
Where Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')
)

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'174' as UT_ID
,CASE WHEN B.Parent_Equipment_No is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_IO_Allocations A
left outer join Sigraph_Silver.S_Major_Equipments B 
ON A.Parent_Equipment_No=B.Parent_Equipment_No
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'175' as UT_ID
,CASE WHEN RD.TagNo is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_IO_Allocations A
LEFT Outer JOIN 
(
Select Distinct TagNo from Sigraph_Silver.S_Instrument_Index
UNION
Select Distinct A.Equipment_No as TagNo from Sigraph_Silver.s_component A
) as  RD ON RD.TagNo=A.Tag_Number
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'176' as UT_ID
,CASE WHEN RD.Equipment_No is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_IO_Allocations A
LEFT Outer JOIN Sigraph_Silver.S_Component RD 
ON RD.Parent_Equipment_No=A.Parent_Equipment_No 
and RD.Equipment_No=A.Equipment_No
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'177' as UT_ID
,CASE WHEN RD.Model_Number is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_IO_Allocations A
LEFT Outer JOIN Sigraph_Silver.S_IO_Catalogue RD 
ON RD.Model_Number=A.CatalogueNo 
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'178' as UT_ID
,CASE WHEN Count(1) over(Partition by A.Parent_Equipment_No,A.Equipment_No) <= TotalChannel 
      then 'Pass' else 'Fail' end as Test_Case
from (
Select Distinct
Tag_Number
,Parent_Equipment_No
,IOType
,Equipment_No
,CatalogueNo
,ChannelNumber
,database_name
,object_identifier
From Sigraph_Silver.S_IO_Allocations
Where Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

) A
LEFT Outer JOIN 
(
      Select Distinct database_name,Object_identifier,Cast(NoOfPoints as Bigint)  as TotalChannel
      From Sigraph_Silver.S_IO_Catalogue
      ) B On A.database_name=B.database_name and A.object_identifier=B.object_identifier;

DELETE FROM SIGRAPH_SILVER.UNIT_TEST_RESULTS WHERE Loader_Name=="IO_ALLOCATIONS";

INSERT INTO SIGRAPH_SILVER.UNIT_TEST_RESULTS (Loader_Name,database_name,Object_Identifier,UT_ID,Test_Case)
SELECT "IO_ALLOCATIONS" AS Loader_Name,
*
FROM UT_VW_IO_ALLOCATIONS;
