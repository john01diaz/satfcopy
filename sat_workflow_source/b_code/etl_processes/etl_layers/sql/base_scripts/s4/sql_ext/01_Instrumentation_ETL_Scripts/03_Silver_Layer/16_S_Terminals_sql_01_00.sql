

Create OR Replace Temp View VW_Terminals_Prep_1
As
Select 
 A.database_name
,A.dynamic_class
,A.object_identifier
,A.Item_Dynamic_Class
,A.Item_object_Identifier
,Coalesce(Location_Designation,'') as Parent_Equipment_No
-- for instrument load the tag number as Equipment no, for rest all load the product key.
,Case When A.Type='Field Device' Then Tag_Number else Product_Key END as Equipment_No
,PM.Terminal_Marking as Marking
,A.Class
From Sigraph_Silver.S_ItemFunction as A 
Inner join Sigraph_silver.S_Pin as PM 
On A.database_name=PM.database_name 
and A.Dynamic_Class=PM.Function_Dynamic_Class
and A.Object_Identifier=PM.Function_Object_Identifier
and PM.Pin_Type='EL_PIN'
Where
-- For PLC Overview function, where Type is IO Module and Channl number is blank then ignore, as these are created for visualization purpose only.
Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1


