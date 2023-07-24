-- Databricks notebook source
-- MAGIC %run /Users/p.abhishek@shell.com/01_Instrumentation_ETL_Scripts/03_Silver_Layer/01_Data_Loaders_Functions

-- COMMAND ----------

-- Get the Pin orientation where we have Electrical Pins in El Pin Class
Create Or Replace Temp View VW_Item_Model_Prep_Query_1
As
Select Distinct
DP.database_name
,DP.Dynamic_Class
,DP.Object_Identifier
,DP.Item_Dynamic_Class
,DP.Item_Object_Identifier
,DP.ModelNo
,DP.Symbol_Name
,Case When DP.item_dynamic_class='LC_Terminal_strip' 
      Then LDP.Terminal_Side
      When LDP.Terminal_Side='Left' Then LDP.Terminal_Marking
      Else ''
      End as Left_Marking
-- As terminal strips pin has only one pin pointed to Left and Right. We should not double count the same in both. So consider only on Left Side.
,Case When DP.item_dynamic_class='LC_Terminal_strip' Then ''
      When LDP.Terminal_Side='Right' Then LDP.Terminal_Marking
      Else ''
      END as Right_Marking
From sigraph_silver.S_ItemFunction DP 
-- Left Marking
Inner join Sigraph_Silver.S_Pin LDP 
ON LDP.database_name=DP.database_name
and LDP.Function_Dynamic_Class=DP.Dynamic_Class
and LDP.Function_Object_Identifier=DP.Object_Identifier
and LDP.Pin_Type='EL_PIN'
-- Consider only those records, where we are getting El pin orientation
Where Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1


-- COMMAND ----------

-- Get the Pin orientation where we have Symbol and Pin orientation defined
Create Or Replace Temp View VW_Item_Model_Prep_Query_2
As
Select Distinct
DP.database_name
,DP.Dynamic_Class
,DP.Object_Identifier
,DP.Item_Dynamic_Class
,DP.Item_Object_Identifier
,DP.ModelNo
,DP.Symbol_Name
,Case When Left_Marking is not null and 
      Dense_rank() Over(Partition by DP.database_name,DP.Item_Object_identifier,All_Marking order by DP.Object_identifier)>1
      Then Concat(
          Row_Number() Over(Partition by DP.database_name,DP.Item_Object_identifier,All_Marking order by DP.Object_identifier)
          ,'_'    
          ,Left_Marking
          )
      Else Coalesce(Left_Marking,'') END as Left_Marking
-- As terminal strips pin has only one pin pointed to Left and Right. We should not double count the same in both. So consider only on Left Side.
,Case When DP.item_dynamic_class='LC_Terminal_strip' Then ''
      When Right_Marking is not null and 
      Dense_rank() Over(Partition by DP.database_name,DP.Item_Object_identifier,All_Marking order by DP.Object_identifier)>1
      Then Concat(
          Row_Number() Over(Partition by DP.database_name,DP.Item_Object_identifier,All_Marking order by DP.Object_identifier)
          ,'_'    
          ,Right_Marking
          )
      Else Coalesce(Right_Marking,'') END as Right_Marking
From sigraph_silver.S_ItemFunction DP 
-- if The pins are not present in El PIN Class, Then consider the pin orientation using below symbol and pin cross walk.
LEFT ANTI JOIN VW_Item_Model_Prep_Query_1 ELP ON ELP.database_name=DP.database_name
and ELP.Item_Dynamic_Class=DP.Item_Dynamic_Class and ELP.Item_Object_Identifier=DP.Item_Object_Identifier

Inner join (
Select
Symbol_Name
,Coalesce(S.Top,S.Right) as Left_Marking
,Coalesce(S.Bottom,S.Left) as Right_Marking
,Coalesce(S.Top,S.Right,S.Unknown,S.Bottom,S.Left) as All_Marking
From sigraph_reference.Symbol_pin_orientation S
Where Coalesce(S.Top,S.Right,S.Bottom,S.Left) is not null
) as SPO ON SPO.Symbol_Name=Case When DP.Item_Dynamic_Class<>'LC_PLC_Module' Then DP.Symbol_Name END
Where Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1


-- COMMAND ----------

-- For IO we need to apply different logic, as it is not based on terminal markings but it is based on number of channels. Below logic takes the not null model no as is, wherever we have modelNo null , it generates the dummy moodelNo. If same model no has two different channel count, then generate that as new version.
Create Or Replace Temp View VW_Item_Function_Model_Extract_IO
As
   
Select Distinct
 A.database_name
,A.Dynamic_Class
,A.Object_Identifier
,A.Item_dynamic_class
,A.Item_Object_Identifier
,A.ModelNo    
,A.Symbol_Name 
,Case When A.Type<>'IO Module' Then Concat(A.ChannelNumber,'+') Else '' END  as Left_Marking
,Case When A.Type='IO Module' Then Concat(A.ChannelNumber,'+') Else '' END  as Right_Marking
from sigraph_silver.S_Itemfunction A
Where Type in ('IO Module','FTA') and A.ChannelNumber is not null and A.ChannelNumber<>'' and A.ChannelNumber<>'0'
and Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1

UNION
-- FTA need to add only +, as we need this to define the connection between IO and FTA.
Select Distinct
 A.database_name
,A.Dynamic_Class
,A.Object_Identifier
,A.Item_dynamic_class
,A.Item_Object_Identifier
,A.ModelNo    
,A.Symbol_Name 
,Case When A.Type<>'IO Module' Then Concat(A.ChannelNumber,'-') Else '' END  as Left_Marking
,Case When A.Type='IO Module' Then Concat(A.ChannelNumber,'-') Else '' END  as Right_Marking
from sigraph_silver.S_Itemfunction A
Where Type in ('IO Module') and A.ChannelNumber is not null and A.ChannelNumber<>'' and A.ChannelNumber<>'0'
and Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1
-- As per the IO rule, each IO should have two terminals.
-- If we have El PIn for PLC Function, but only one marking is there then add additional marking using below query



-- COMMAND ----------


Create Or Replace Temp View VW_Item_Function_Model_Extract
As
-- Load only delta. If the data is already loaded in S_Item_Function_model table, then do not update the ModelNo.

SELECT DISTINCT
 DP.database_name
,DP.Dynamic_Class
,DP.Object_Identifier
,DP.Item_dynamic_class
,DP.Item_Object_Identifier
,DP.Left
,DP.Right
,DP.Left_Marking
,DP.Right_Marking
,DP.All_Marking
,DP.ModelNO as ModelNo_Original
,Case When DP.ModelNO is null 
      Then Concat('RHLDD',1000+Dense_Rank() Over(order by All_Marking))
     When Dense_Rank() Over(Partition by DP.ModelNO order by All_Marking) > 1 
     Then Concat(ModelNO,' - V',Dense_Rank() Over(Partition by ModelNO order by All_Marking))
     Else ModelNO
     END as ModelNO 
,DP.Symbol_Name
From (
SELECT DISTINCT
 DP.database_name
,DP.Dynamic_Class
,DP.Object_Identifier
,DP.Item_dynamic_class
,DP.Item_Object_Identifier
,size(array_elements_sorting(
      collect_set(Left_Marking) 
      OVER(PARTITION BY DP.database_name,DP.Item_object_Identifier))) AS Left
,size(array_elements_sorting(
      collect_set(Right_Marking) 
      OVER(PARTITION BY DP.database_name,DP.Item_object_Identifier))) AS Right
-- Get the comma separeted values      
,to_get_sort(collect_set(Left_Marking) 
            over(partition by DP.database_name,DP.Item_object_Identifier)) as Left_Marking
,to_get_sort(collect_set(Right_Marking) 
            over(partition by DP.database_name,DP.Item_object_Identifier)) as Right_Marking   
,to_get_sort(collect_set(MD.All_Marking) 
            over(partition by DP.database_name,DP.Item_object_Identifier)) as All_Marking            
,DP.ModelNo
  
,DP.Symbol_Name             
From (
Select * from VW_Item_Model_Prep_Query_1
UNION
Select * from VW_Item_Model_Prep_Query_2
UNION
Select * from VW_Item_Function_Model_Extract_IO
) as DP
-- Get all the terminal markings in one column. This is required to check the unique model and its terminal marking
Inner join (
SELECT Database_name,Dynamic_Class,Object_Identifier,Left_Marking as All_Marking from VW_Item_Model_Prep_Query_1
UNION
SELECT Database_name,Dynamic_Class,Object_Identifier,Right_Marking as All_Marking from VW_Item_Model_Prep_Query_1
UNION
SELECT Database_name,Dynamic_Class,Object_Identifier,Left_Marking as All_Marking from VW_Item_Model_Prep_Query_2
UNION
SELECT Database_name,Dynamic_Class,Object_Identifier,Right_Marking as All_Marking from VW_Item_Model_Prep_Query_2
UNION
SELECT Database_name,Dynamic_Class,Object_Identifier,Left_Marking as All_Marking from VW_Item_Function_Model_Extract_IO
UNION
SELECT Database_name,Dynamic_Class,Object_Identifier,Right_Marking as All_Marking from VW_Item_Function_Model_Extract_IO
) as MD ON MD.Database_name=DP.database_name 
and MD.Dynamic_Class=DP.Dynamic_Class and MD.Object_Identifier=DP.Object_identifier

) as DP



-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.sql('Select * from VW_Item_Function_Model_Extract where database_name == "R_2016R3"')
-- MAGIC
-- MAGIC dbutils.fs.rm('dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Item_Function_Model',True)
-- MAGIC
-- MAGIC df.write.save(
-- MAGIC      format          = "delta"
-- MAGIC     ,path            = 'dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Item_Function_Model'
-- MAGIC     ,mode            = 'overwrite'
-- MAGIC     ,overwriteSchema = True
-- MAGIC )

-- COMMAND ----------

Select * from VW_Item_Model_Prep_Query_2 Where object_identifier='ID_28_c_1b36fc8'