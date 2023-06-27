-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW UT_VW_COMPONENT
AS
Select 
 A.database_name
,A.object_identifier
,'142' as UT_ID
,CASE WHEN B.Object_Identifier is not null then 'Pass' else 'Fail' end as Test_Case
from Sigraph_silver.s_itemfunction A
LEFT SEMI JOIN Sigraph_Silver.S_Item_Function_Model FM ON FM.database_name=A.Database_name 
and FM.Object_Identifier=A.Object_Identifier
Left outer join sigraph_Silver.S_Component B On A.database_name=B.database_name and A.Object_Identifier=B.Object_Identifier
Where A.Type in ('FTA','Device','Terminal Strips')
-- in PLC Module for PLC Overview class we dont have channel number. We no need to load this data.
and A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select 
 A.database_name
,A.object_identifier
,'142' as UT_ID
,CASE WHEN B.Object_Identifier is not null then 'Pass' else 'Fail' end as Test_Case
from Sigraph_silver.s_itemfunction A
LEFT SEMI JOIN Sigraph_Silver.S_IO_Catalogue FM ON FM.database_name=A.Database_name 
and FM.Object_Identifier=A.Object_Identifier
Left outer join sigraph_Silver.S_Component B On A.database_name=B.database_name and A.Object_Identifier=B.Object_Identifier
Where A.Type in ('IO Module')
-- in PLC Module for PLC Overview class we dont have channel number. We no need to load this data.
and A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select 
 A.database_name
,A.object_identifier
,'143' as UT_ID
,CASE WHEN B.Parent_Equipment_No is not null then 'Pass' else 'Fail' end as Test_Case
from sigraph_Silver.S_Component A

Left outer join sigraph_Silver.s_major_equipments B On A.database_name=B.database_name 
and A.parent_Equipment_No=B.Parent_Equipment_No
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select 
 A.database_name
,A.object_identifier
,'144' as UT_ID
,CASE WHEN EquipmentType in ('Device', 'Terminal Strip','IO Module') then 'Pass' else 'Fail' end as Test_Case
from sigraph_Silver.S_Component A
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select 
 A.database_name
,A.object_identifier
,'145' as UT_ID
,CASE WHEN B.CatalogueNo is not null then 'Pass' else 'Fail' end as Test_Case
from sigraph_Silver.S_Component A
Left outer join (
Select Distinct Model_Number as CatalogueNo,'IO Module' as EquipmentType From sigraph_Silver.s_io_catalogue
UNION
Select Distinct ModelNo as CatalogueNo,'Device' as EquipmentType From sigraph_Silver.S_DeviceCatalogue
) as B On A.EquipmentType=B.EquipmentType and A.CatalogueNo=B.CatalogueNo
Where A.CatalogueNo is not null
and A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select 
 database_name
,object_identifier
,'146' as UT_ID
,CASE WHEN Row_Number() Over(Partition by Parent_Equipment_No,DinRail,Sequence order by Sequence)=1 then 'Pass' else 'Fail' end as Test_Case
from sigraph_Silver.S_Component A
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

