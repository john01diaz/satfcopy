-- Databricks notebook source
-- MAGIC %run /Users/p.abhishek@shell.com/01_Instrumentation_ETL_Scripts/06_ETL_Execution_Packages/Databases_Configuration

-- COMMAND ----------

Select Distinct
From_Parent_Equipment_No
,From_Compartment
,From_Equipment
,From_Wire_Link
,From_Marking
,From_Left_Right
,To_Parent_Equipment_No
,To_Compartment
,To_Equipment_No
,To_Wire_Link
,To_Marking
,To_Left_Right  
From Sigraph_Silver.S_Internal_Wiring
Where Class in ('Instrumentation','Inst(Shared)','Elec(Shared)') 
and database_name in (Select Database_name from VW_Database_names)