CREATE OR REPLACE TEMP VIEW UT_VW_CABLE_CORE_CATALOGUE
AS
Select 
 A.database_name 
,A.object_identifier
,'113' as UT_ID
,CASE WHEN B.Object_Identifier is not null then 'Pass' else 'Fail' end as Test_Case
from Sigraph.Wire_Function A
Left outer join Sigraph_Silver.S_CableCoreCatalogue B 
On A.database_name=B.database_name 
and A.Object_Identifier=B.Object_Identifier

UNION

Select 
 A.database_name 
,A.object_identifier
,'114' as UT_ID
,CASE WHEN B.Object_Identifier is not null then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_CableCoreCatalogue A
Left Outer join Sigraph_Silver.S_CableCatalogue B 
On A.database_name=B.database_name 
and A.Cable_Object_Identifier=B.Object_Identifier
and B.EarthCore='TRUE'
Where A.Core_Markings_Core_Type='E'

UNION

Select 
 A.database_name 
,A.object_identifier
,'115' as UT_ID
,CASE WHEN B.Object_Identifier is not null then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_CableCoreCatalogue A
Left Outer join Sigraph_Silver.S_CableCatalogue B On A.database_name=B.database_name and A.Cable_Object_Identifier=B.Object_Identifier
and B.OAScr='TRUE'
Where A.Core_Markings_Core_Type='OAS'

UNION

Select 
 A.database_name 
,A.object_identifier
,'116' as UT_ID
,Case When Remarks is not null Then 'Pass' 
      When 
      CASE 
      WHEN NoOfGroups=1 Then 1
      WHEN A.GroupType='Cores' Then 1
      Else NoOfGroups
      END
      =
      MAX(Cast(Case When  A.GroupType='Cores' Then 1 Else Group_Marking END as Bigint)) Over(Partition by A.database_name,A.Cable_Object_Identifier)
      Then 'Pass'
      Else 'Fail'
      END as Test_Case
from Sigraph_Silver.S_CableCoreCatalogue A
Inner join Sigraph_Silver.S_CableCatalogue B On A.database_name=B.database_name and A.Cable_Object_Identifier=B.Object_Identifier
Where A.Core_Markings_Core_Type='C' and A.Group_Marking<>999

UNION

Select 
 A.database_name 
,A.object_identifier
,'117' as UT_ID
,CASE WHEN B.Object_Identifier is null then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_CableCatalogue A
Left outer join (
Select database_name,object_identifier from Sigraph_Silver.S_CableCoreCatalogue 
Group by database_name,object_identifier Having Count(1)>1
) B On A.database_name=B.database_name and A.Object_Identifier=B.Object_Identifier

UNION

Select 
 A.database_name 
,A.object_identifier
,'118' as UT_ID
,CASE WHEN B.Object_Identifier is not null then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_CableCoreCatalogue A
Left Outer join Sigraph_Silver.S_CableCatalogue B On A.database_name=B.database_name and A.Cable_Object_Identifier=B.Object_Identifier
and B.GroupScr='TRUE'
Where A.Core_Markings_Core_Type='S'

UNION

Select 
 A.database_name 
,A.object_identifier
,'119' as UT_ID
,CASE WHEN Row_Number() Over(Partition by database_name,object_identifier,Core_Markings order by Group_Marking_Sequence)=1 then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_CableCoreCatalogue A



UNION

Select 
 A.database_name 
,A.object_identifier
,'120' as UT_ID
,Case When Row_Number() Over(Partition by database_name,object_identifier,Group_Marking,Group_Marking_Sequence order by Group_Marking_Sequence)=1
      Then 'Pass'
      Else 'Fail'
      END as Test_Case
from Sigraph_Silver.S_CableCoreCatalogue A

UNION

Select 
 A.database_name 
,A.object_identifier
,'121' as UT_ID
,Case When Row_Number() Over(Partition by database_name,object_identifier,Group_Marking,Group_Marking_Sequence order by Group_Marking_Sequence)=1
      Then 'Pass'
      Else 'Fail'
      END as Test_Case
from Sigraph_Silver.S_CableCoreCatalogue A



DELETE FROM SIGRAPH_SILVER.UNIT_TEST_RESULTS WHERE Loader_Name=="CABLE_CORE_CATALOGUE";

INSERT INTO SIGRAPH_SILVER.UNIT_TEST_RESULTS (Loader_Name,database_name,Object_Identifier,UT_ID,Test_Case)
SELECT "CABLE_CORE_CATALOGUE" AS Loader_Name,
*
FROM UT_VW_CABLE_CORE_CATALOGUE



