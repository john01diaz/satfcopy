-- Databricks notebook source
-- MAGIC %run /Users/p.abhishek@shell.com/01_Instrumentation_ETL_Scripts/06_ETL_Execution_Packages/Databases_Configuration

-- COMMAND ----------


Select Distinct
A.Area
,A.Parent_Equipment_No
,A.Description
,A.EquipmentType 
,A.VendorSupplied
,A.DwgRequired
,A.Status
,A.AreaPath
,A.Type
,A.Designation
,A.Comment
,A.Installation_site as `Installation site`
,A.Category
From Sigraph_Silver.S_Major_Equipments A
Where database_name in (Select Database_name from VW_Database_names)

-- COMMAND ----------

