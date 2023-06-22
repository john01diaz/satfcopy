-- Databricks notebook source
Create or replace temp view VW_Database_names
As
Select 
explode(Split('R_2016R3',',')) as Database_name