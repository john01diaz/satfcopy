CREATE OR REPLACE TEMP VIEW UT_VW_TERMINALS
AS
Select Distinct
 A.database_name
,A.object_identifier
,'151' as UT_ID
,CASE WHEN B.Object_Identifier is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_ItemFunction A
LEFT SEMI JOIN Sigraph_Silver.S_Item_Function_Model FM ON FM.database_name=A.Database_name 
and FM.Object_Identifier=A.Object_Identifier
left outer join Sigraph_Silver.S_Terminals B
ON A.database_name=B.database_name
and A.Dynamic_Class=B.Dynamic_Class 
and A.Object_Identifier=B.Object_Identifier
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')
and A.Type in ('Device','FTA','Terminal Strip')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'151' as UT_ID
,CASE WHEN B.Object_Identifier is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_ItemFunction A
LEFT SEMI JOIN Sigraph_Silver.S_IO_Catalogue FM ON FM.database_name=A.Database_name 
and FM.dynamic_class=A.Dynamic_Class
and FM.Object_Identifier=A.Object_Identifier
left outer join Sigraph_Silver.S_Terminals B
ON A.database_name=B.database_name
and A.Dynamic_Class=B.Dynamic_Class 
and A.Object_Identifier=B.Object_Identifier
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')
and A.Type in ('IO Module')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'152' as UT_ID
,CASE WHEN Row_Number() Over(Partition by Database_name, Dynamic_Class, Object_Identifier,Marking Order by Object_Identifier)=1
      Then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Terminals A
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'153' as UT_ID
,CASE WHEN B.Parent_Equipment_No is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Terminals A
left outer join Sigraph_Silver.S_Major_Equipments B 
ON A.Parent_Equipment_No=B.Parent_Equipment_No
Where A.Parent_Equipment_No<>''
and A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'154' as UT_ID
,CASE WHEN RD.Equipment_No is not null  then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Terminals A
LEFT Outer JOIN (
Select 
TagNo as Equipment_No 
from Sigraph_Silver.S_Instrument_Index
UNION
Select Equipment_No 
from Sigraph_Silver.S_Component
) as RD 
ON RD.Equipment_No=A.Equipment_No
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'155' as UT_ID
,CASE WHEN   B.Object_Identifier is not null
      Then 'Pass' 
      Else 'Fail' End as Test_Case
from Sigraph_Silver.S_Terminals A
Inner JOIN (
Select Distinct
Database_name
,From_Dynamic_Class as Dynamic_Class
,From_Object_Identifier as Object_Identifier
,Coalesce(From_Location,'') as Parent_Equipment_No
,Coalesce(From_Item,'') as Equipment_No
,Coalesce(From_Terminal_marking,'')  as Marking 
from Sigraph_Silver.S_Connection
UNION
Select 
Database_name
,To_Dynamic_Class as Dynamic_Class
,To_Object_Identifier as Object_Identifier
,Coalesce(To_Location,'') as Parent_Equipment_No
,Coalesce(To_Item,'') as Equipment_No
,Coalesce(To_Terminal_marking,'')  as Marking 
from Sigraph_Silver.S_Connection
) as B 
ON A.Database_name=B.Database_name and A.Dynamic_Class=B.Dynamic_Class and A.Object_Identifier=B.Object_Identifier
and B.Parent_Equipment_No=A.Parent_Equipment_No and B.Equipment_No=A.Equipment_No and B.Marking=A.Marking
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 database_name
,object_identifier
,'156' as UT_ID
,CASE WHEN Row_Number() Over(Partition by Parent_Equipment_No,Equipment_No,Marking order by Sequence)=1 then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Terminals A
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'179' as UT_ID
,CASE WHEN B.ModelNo is not null  then 'Pass' else 'Fail' end as Test_Case
From (
Select Distinct A.Database_name,A.Dynamic_Class,A.Object_identifier,B.CatalogueNo,A.Marking
from Sigraph_Silver.S_Terminals A
Inner join Sigraph_Silver.S_Component B ON A.Database_name=B.Database_name and A.Dynamic_Class=B.Dynamic_Class
and A.Object_Identifier=B.Object_Identifier
Where A.database_name='R_2016R3' and EquipmentType='Device'  and A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)') 
) as A
Left Outer join (
Select Distinct
ModelNo,
explode(split(Left_Marking,',')) as Marking
from Sigraph_silver.S_DeviceCatalogue
Where database_name='R_2016R3' and Catalogue_RNT = 1 and Class in ('Instrumentation','Inst(Shared)','Elec(Shared)') 

UNION
Select Distinct
ModelNo,
explode(split(Right_Marking,',')) as Marking
from Sigraph_silver.S_DeviceCatalogue
Where database_name='R_2016R3' and Catalogue_RNT = 1 and Class in ('Instrumentation','Inst(Shared)','Elec(Shared)') 
) as B ON A.CatalogueNo=B.ModelNo and A.Marking=B.Marking

UNION


Select Distinct
 A.database_name
,A.object_identifier
,'184' as UT_ID
,CASE WHEN B.Model_Number is not null  then 'Pass' else 'Fail' end as Test_Case
From (
Select Distinct A.Database_name,A.Dynamic_Class,A.Object_identifier,B.CatalogueNo,A.Marking
from Sigraph_Silver.S_Terminals A
Inner join Sigraph_Silver.S_Component B ON A.Database_name=B.Database_name and A.Dynamic_Class=B.Dynamic_Class
and A.Object_Identifier=B.Object_Identifier
Where A.database_name='R_2016R3' and EquipmentType='IO Module'  
and A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)') 
) as A
Left Outer join  Sigraph_silver.S_IO_Catalogue B ON B.Catalogue_RNT = 1 
and B.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)') 
and A.CatalogueNo=B.Model_Number and A.Marking=B.TerminalsPerMarking



DELETE FROM SIGRAPH_SILVER.UNIT_TEST_RESULTS WHERE Loader_Name=="TERMINALS";

INSERT INTO SIGRAPH_SILVER.UNIT_TEST_RESULTS (Loader_Name,database_name,Object_Identifier,UT_ID,Test_Case)
SELECT "TERMINALS" AS Loader_Name,
*
FROM UT_VW_TERMINALS
