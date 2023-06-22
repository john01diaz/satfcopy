

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



