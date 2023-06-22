-- Databricks notebook source
-- MAGIC %run /Users/p.abhishek@shell.com/01_Instrumentation_ETL_Scripts/06_ETL_Execution_Packages/Databases_Configuration

-- COMMAND ----------

Select 
A.Parent_Equipment_No
,A.Equipment_No
,A.EquipmentType
,A.CatalogueNo
,A.DinRail
,Row_Number() Over(Partition by Parent_Equipment_No,DinRail order by Item_Object_identifier) as Sequence
,Remarks  
From (
Select Distinct
A.Parent_Equipment_No
,A.Equipment_No
,A.EquipmentType
,A.CatalogueNo
,A.DinRail
,A.Remarks  
,A.Item_Object_identifier
From Sigraph_Silver.S_Component A
Where A.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)') and database_name in (Select Database_name from VW_Database_names) 
) as A