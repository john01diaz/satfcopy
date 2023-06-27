

-- Terminal markings for the devices, where it does have pin information in the pin class
Create OR Replace Temp View VW_Terminals_Prep_1
As
Select 
 A.database_name
,A.dynamic_class
,A.object_identifier
,A.Item_Dynamic_Class
,A.Item_object_Identifier
,Coalesce(Location_Designation,'') as Parent_Equipment_No
,A.Product_Key as Equipment_No
,PM.Terminal_Marking as Marking
,A.Class
From Sigraph_Silver.S_ItemFunction as A 
Inner join Sigraph_silver.S_Pin as PM 
On A.database_name=PM.database_name 
and A.Dynamic_Class=PM.Function_Dynamic_Class
and A.Object_Identifier=PM.Function_Object_Identifier
and PM.Pin_Type='EL_PIN'
Where A.Type in ('Device','FTA','Terminal Strip')


