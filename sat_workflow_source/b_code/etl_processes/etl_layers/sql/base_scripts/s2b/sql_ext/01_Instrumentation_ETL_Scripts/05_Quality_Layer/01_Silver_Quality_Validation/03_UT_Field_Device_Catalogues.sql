CREATE OR REPLACE TEMP VIEW UT_VW_FDC 
AS
select
 database_name 
,object_identifier
,'100' as UT_ID
,CASE WHEN Coalesce(`Catalogue_Name`,'')<>'' then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Field_Device_Catalogue
Where Catalogue_RNT=1 and Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')
and (Coalesce(Left_pin_details,0)+Coalesce(Right_pin_details,0))>0

union

Select 
 database_name
,object_identifier
,'101' as UT_ID
,CASE WHEN Length(Coalesce(`Catalogue_Name`,''))<=40 then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Field_Device_Catalogue 
Where Catalogue_RNT=1 and Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select 
 database_name
,object_identifier
,'102' as UT_ID
,CASE WHEN Row_Number() Over(Partition by Coalesce(`Catalogue_Name`,'') order by Coalesce(`Catalogue_Name`,''))=1 then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_Field_Device_Catalogue
Where Catalogue_RNT=1 and Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')



UNION

Select 
 database_name 
,object_identifier
,'103' as UT_ID
,CASE WHEN Left_pin_details<>0 and Left_pin_details=(Length(Left_Marking)-Length(replace(Left_Marking,',','')))+1 And
           Right_pin_details<>0 and Right_pin_details=(Length(Right_Marking)-Length(replace(Right_Marking,',','')))+1
      Then 'Pass' 
      WHEN Left_pin_details<>0 and Left_pin_details=(Length(Left_Marking)-Length(replace(Left_Marking,',','')))+1 And
           Right_pin_details=0
      Then 'Pass'
      WHEN Left_pin_details=0 And
           Right_pin_details<>0 and Right_pin_details=(Length(Right_Marking)-Length(replace(Right_Marking,',','')))+1
      Then 'Pass'
      WHEN Left_pin_details=0 And Right_pin_details=0 and Left_Marking='' and Right_Marking=''
      Then 'Pass'
      Else 'Fail' end as Test_Case
from Sigraph_Silver.S_Field_Device_Catalogue
Where Catalogue_RNT=1 and Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')

UNION

Select  
database_name 
,object_identifier
,'183' as UT_ID 
,CASE WHEN row_number() Over(PARTITION BY A.database_name,A.Object_Identifier,Marking order by Marking)=1 
      then 'Pass' else 'Fail' end as Test_Case
From (
Select Distinct
database_name
,Object_Identifier
,explode(Split(Concat(Left_Marking,',',Right_Marking),','))as Marking
FROM sigraph_silver.s_field_device_catalogue
Where Catalogue_RNT=1 and Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')
) as A





INSERT INTO SIGRAPH_SILVER.UNIT_TEST_RESULTS (Loader_Name,database_name,Object_Identifier,UT_ID,Test_Case)
SELECT "FIELD_DEVICE_CATALOGUE" AS Loader_Name,
*
FROM UT_VW_FDC
