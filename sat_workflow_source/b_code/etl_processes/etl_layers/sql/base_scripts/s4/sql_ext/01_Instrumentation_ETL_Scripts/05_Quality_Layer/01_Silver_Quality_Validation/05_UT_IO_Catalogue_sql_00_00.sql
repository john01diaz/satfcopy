-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW UT_VW_IO_CATALOGUE
AS
Select 
 database_name
,object_identifier
,"122" as UT_ID
,CASE WHEN Coalesce(Model_Number,'')<>'' then 'Pass' else 'Fail' end as Test_Case
from Sigraph_Silver.S_IO_Catalogue
Where Catalogue_RNT=1

UNION

Select 
 database_name
,object_identifier
,"123" as UT_ID
,CASE WHEN Row_Number() Over(Partition by Model_Number,IOType,TerminalsPerMarking order by Model_Number)=1 then 'Pass' else 'Fail' end as TestCase
from Sigraph_Silver.S_IO_Catalogue 
Where Catalogue_RNT=1 

UNION

Select 
 database_name
,object_identifier
,"123" as UT_ID
,CASE WHEN Row_Number() Over(Partition by Item_object_identifier,TerminalsPerMarking order by object_identifier)=1 then 'Pass' else 'Fail' end as TestCase
from Sigraph_Silver.S_IO_Catalogue 

UNION

Select 
 database_name
,object_identifier
,"190" as UT_ID
,CASE WHEN Count(1) Over(Partition by database_name,Item_object_identifier,IOType)=NoOfPoints*TerminalsPerPointChannel then 'Pass' else 'Fail' end as TestCase
from Sigraph_Silver.S_IO_Catalogue 
Where Catalogue_RNT=1 

