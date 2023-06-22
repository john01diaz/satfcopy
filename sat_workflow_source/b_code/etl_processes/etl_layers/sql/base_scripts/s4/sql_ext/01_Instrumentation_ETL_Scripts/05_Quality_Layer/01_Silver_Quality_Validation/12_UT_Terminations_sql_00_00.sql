-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW UT_VW_TERMINATIONS
AS
Select Distinct
 A.database_name
,A.object_identifier
,'157' as UT_ID
,CASE WHEN B.Object_Identifier is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_silver.S_Connection A
left outer join Sigraph_Silver.S_Terminations B 
ON A.database_name=B.database_name 
and A.Object_Identifier=B.Object_Identifier
WHERE Coalesce(A.From_Location,'')<>Coalesce(A.TO_Location,'')


UNION

Select Distinct
 database_name
,object_identifier
,'158' as UT_ID
,CASE WHEN Count(1) Over(Partition by Database_name, Object_Identifier,Left_Right Order by Object_Identifier)=1
      Then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Terminations A
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'159' as UT_ID
,CASE WHEN B.Parent_Equipment_No is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Terminations A
left outer join (
Select
Coalesce(From_Location,'') as Parent_Equipment_No
,CableNumber
from Sigraph_Silver.S_CableSchedule 
UNION
Select
Coalesce(To_Location,'') as Parent_Equipment_No
,CableNumber
from Sigraph_Silver.S_CableSchedule 
) as B ON A.Parent_Equipment_No=B.Parent_Equipment_No and A.CableNumber=B.CableNumber
Where A.Parent_Equipment_No<>''
and A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'160' as UT_ID
,CASE WHEN B.Core_Markings is not null then 'Pass' else 'Fail' end as TestCase
from Sigraph_Silver.S_Terminations A
left outer join (
    Select
     Coalesce(C.From_Location,'') as Parent_Equipment_No
    ,C.CableNumber
    ,CC.Core_Markings
    from Sigraph_Silver.S_CableSchedule C
    Inner join Sigraph_Silver.S_CableCoreCatalogue CC 
    ON CC.database_name=C.Database_name 
    and CC.Cable_Object_Identifier=C.Object_identifier
    
    UNION
    
    Select
      Coalesce(C.To_Location,'') as Parent_Equipment_No
     ,C.CableNumber
     ,CC.Core_Markings
     from Sigraph_Silver.S_CableSchedule C
     Inner join Sigraph_Silver.S_CableCoreCatalogue CC 
     ON CC.database_name=C.Database_name 
     and CC.Cable_Object_Identifier=C.Object_identifier
) as B 
ON A.Parent_Equipment_No=B.Parent_Equipment_No 
and A.CableNumber=B.CableNumber 
and A.Core_Markings=B.Core_Markings
Where A.CableNumber not like '_%ID_%'
and A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'161' as UT_ID
,CASE WHEN B.Parent_Equipment_No is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Terminations A
left outer join Sigraph_Silver.S_Major_Equipments B 
ON A.Parent_Equipment_No=B.Parent_Equipment_No
Where A.Parent_Equipment_No<>''
and A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'162' as UT_ID
,CASE WHEN RD.Equipment_No is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Terminations A
LEFT Outer JOIN (
Select 
'' as Parent_Equipment_No
,TagNo as Equipment_No 
from Sigraph_Silver.S_Instrument_Index
UNION
Select Parent_Equipment_No
,Equipment_No 
from Sigraph_Silver.S_Component
) as RD 
ON RD.Parent_Equipment_No=A.Parent_Equipment_No 
and RD.Equipment_No=A.Equipment_No
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'163' as UT_ID
,CASE WHEN B.Marking is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Terminations A
LEFT Outer JOIN Sigraph_Silver.S_Terminals B ON A.Parent_Equipment_No=B.Parent_Equipment_No 
and A.Equipment_No=B.Equipment_No and A.Marking=B.Marking
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

