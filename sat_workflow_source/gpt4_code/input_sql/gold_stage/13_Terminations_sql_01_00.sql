-- Databricks notebook source
-- MAGIC %run /Users/p.abhishek@shell.com/01_Instrumentation_ETL_Scripts/06_ETL_Execution_Packages/Databases_Configuration

-- COMMAND ----------

Select Distinct
CableNumber
,Core_Markings
,Parent_Equipment_No
,Equipment_No
,Marking
,Left_Right
From Sigraph_Silver.S_Terminations
Where Class in ('Instrumentation','Inst(Shared)','Elec(Shared)') 
and database_name in (Select Database_name from VW_Database_names)