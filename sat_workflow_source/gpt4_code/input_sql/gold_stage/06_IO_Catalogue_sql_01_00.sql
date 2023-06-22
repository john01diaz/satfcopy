-- Databricks notebook source
-- MAGIC %run /Users/p.abhishek@shell.com/01_Instrumentation_ETL_Scripts/06_ETL_Execution_Packages/Databases_Configuration

-- COMMAND ----------

SELECT Distinct
Model_Number
,Description
,Manufacturer
,DescriptionDrawing
,Channel
,AllowUse
,IOType
,NoOfPoints
,TerminalsPerPointChannel
,TerminalsPerMarking
FROM SIGRAPH_SILVER.S_IO_CATALOGUE
Where Catalogue_RNT=1 
and database_name in (Select Database_name from VW_Database_names)
order by Model_Number,Cast(Replace(Replace(TerminalsPerMarking,'+',''),'-','') as BIGINT)
