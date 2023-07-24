-- Databricks notebook source
-- MAGIC %run /Users/p.abhishek@shell.com/01_Instrumentation_ETL_Scripts/06_ETL_Execution_Packages/Databases_Configuration

-- COMMAND ----------

Select Distinct
Tag_Number
,Parent_Equipment_No
,IOType
,Equipment_No
,CatalogueNo
,ChannelNumber
From Sigraph_Silver.S_IO_Allocations
Where Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')
and database_name in (Select Database_name from VW_Database_names)