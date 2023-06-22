-- Databricks notebook source
-- MAGIC %run /Users/p.abhishek@shell.com/01_Instrumentation_ETL_Scripts/06_ETL_Execution_Packages/Databases_Configuration

-- COMMAND ----------

Select
Parent_Equipment_No
,Equipment_No
,Marking
,Dense_Rank() Over(Partition by Parent_Equipment_No,Equipment_No order by Marking) as Sequence
From (
Select Distinct
Parent_Equipment_No
,Equipment_No
,Marking
From Sigraph_Silver.S_Terminals
Where Class in ('Instrumentation','Inst(Shared)','Elec(Shared)') and 
database_name in (Select Database_name from VW_Database_names)
) as A