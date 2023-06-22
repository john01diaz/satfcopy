-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW UT_VW_INTERNAL_WIRING
AS
Select Distinct
   A.database_name
  ,A.object_identifier
  ,'164' as UT_ID
  ,CASE WHEN B.Object_Identifier is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_silver.S_Connection A
left outer join Sigraph_Silver.S_Internal_Wiring B 
ON A.database_name=B.database_name
and A.Object_Identifier=B.Object_Identifier
WHERE A.From_Location is not null 
and A.TO_Location is not null 
and A.From_Location = A.TO_Location


UNION

Select Distinct
   database_name
  ,object_identifier
  ,'165' as UT_ID
  ,CASE WHEN Row_Number() Over(Partition by From_Parent_Equipment_No,From_Equipment,From_Marking
                              ,To_Parent_Equipment_No,To_Equipment_No,To_Marking Order by Object_Identifier)=1
      Then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Internal_Wiring A
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'166' as UT_ID
,CASE WHEN B.Parent_Equipment_No is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Internal_Wiring A
left outer join Sigraph_Silver.S_Major_Equipments B 
ON A.From_Parent_Equipment_No=B.Parent_Equipment_No
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'167' as UT_ID
,CASE WHEN B.Parent_Equipment_No is not null  then 'Pass' else 'Fail' end as TestCase
from Sigraph_Silver.S_Internal_Wiring A
left outer join Sigraph_Silver.S_Major_Equipments B 
ON A.To_Parent_Equipment_No=B.Parent_Equipment_No
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'168' as UT_ID
,CASE WHEN RD.Equipment_No is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Internal_Wiring A
LEFT Outer JOIN Sigraph_Silver.S_Component RD
ON RD.Parent_Equipment_No=A.From_Parent_Equipment_No 
and RD.Equipment_No=A.From_Equipment
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'169' as UT_ID
,CASE WHEN RD.Equipment_No is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Internal_Wiring A
LEFT Outer JOIN Sigraph_Silver.S_Component RD
ON RD.Parent_Equipment_No=A.To_Parent_Equipment_No 
and RD.Equipment_No=A.To_Equipment_No
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'170' as UT_ID
,CASE WHEN B.Marking is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Internal_Wiring A
LEFT Outer JOIN Sigraph_Silver.S_Terminals B 
ON A.From_Parent_Equipment_No=B.Parent_Equipment_No 
and A.From_Equipment=B.Equipment_No
and A.From_Marking=B.Marking
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'171' as UT_ID
,CASE WHEN B.Marking is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Internal_Wiring A
LEFT Outer JOIN Sigraph_Silver.S_Terminals B 
ON A.To_Parent_Equipment_No=B.Parent_Equipment_No 
and A.To_Equipment_No=B.Equipment_No 
and A.To_Marking=B.Marking
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

