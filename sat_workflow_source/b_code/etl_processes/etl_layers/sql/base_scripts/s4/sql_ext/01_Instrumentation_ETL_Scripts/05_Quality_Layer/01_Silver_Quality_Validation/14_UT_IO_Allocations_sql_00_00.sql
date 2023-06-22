-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW UT_VW_IO_ALLOCATIONS
AS
Select Distinct
 A.database_name
,A.object_identifier
,'172' as UT_ID
,CASE WHEN B.Object_Identifier is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_silver.S_ItemFunction A
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
from Sigraph_Silver.S_IO_Allocations A
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

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
LEFT Outer JOIN Sigraph_Silver.S_Instrument_Index RD ON RD.TagNo=A.Tag_Number
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
,CASE WHEN Count(1) over(Partition by A.Parent_Equipment_No,A.Equipment_No) <= TotalChannel  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_IO_Allocations A
LEFT Outer JOIN (
Select Distinct Model_Number,cast(NoOfPoints as Bigint) as TotalChannel
From Sigraph_Silver.S_IO_Catalogue
) as RD ON RD.Model_Number=A.CatalogueNo
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

