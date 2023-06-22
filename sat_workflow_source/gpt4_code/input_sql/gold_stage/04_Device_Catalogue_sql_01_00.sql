-- Databricks notebook source
-- MAGIC %run /Users/p.abhishek@shell.com/01_Instrumentation_ETL_Scripts/03_Silver_Layer/01_Data_Loaders_Functions

-- COMMAND ----------

Select Distinct
  AllowUse,
  Type,
  Description,
  Manufacturer,
  ModelNo,
  Class,
  Left,
  Right,
  Left_Marking,
  Right_Marking,
  Symbol_name,
  Product_Key,
  Loop_Number,
  Tag_Number,
  Document_Number
from
  Sigraph_silver.S_DeviceCatalogue
Where 
  Catalogue_RNT = 1
  and Class in (
    'Instrumentation',
    'Inst(Shared)',
    'Elec(Shared)'
  )
  and database_name in (Select Database_name from VW_Database_names)