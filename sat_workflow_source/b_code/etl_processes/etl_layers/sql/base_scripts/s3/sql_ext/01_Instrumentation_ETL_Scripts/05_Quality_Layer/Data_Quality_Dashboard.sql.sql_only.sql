-- Databricks notebook source
SELECT 
A.Table_Name
,B.TEST_CASE
FROM Sigraph_SILVER.Unit_Test_Master A
INNER JOIN SIGRAPH_SILVER.UNIT_TEST_RESULTS B
ON A.UT_ID == B.UT_ID;

Select *
from Sigraph_gold.UNIT_TEST_RESULTS