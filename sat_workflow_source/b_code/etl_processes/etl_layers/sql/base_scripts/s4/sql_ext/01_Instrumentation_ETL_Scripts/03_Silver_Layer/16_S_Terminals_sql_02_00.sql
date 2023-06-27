

-- Terminal markings for the devices, where it does not have pin information in the pin class
Create OR Replace Temp View VW_Terminals_Prep_2
As

Select 
 A.database_name
,A.dynamic_class
,A.object_identifier
,A.Item_Dynamic_Class
,A.Item_object_Identifier
,Coalesce(Location_Designation,'') as Parent_Equipment_No
-- for instrument load the tag number as Equipment no, for rest all load the product key.
,A.Product_Key as Equipment_No
,Case When Dense_rank() Over(Partition by A.database_name,A.Item_Object_identifier
          ,Coalesce(SPO.Top,SPO.Right,SPO.Unknown,SPO.Bottom,SPO.Left) order by A.Object_identifier)>1
      Then Concat(
          Row_Number() Over(Partition by A.database_name,A.Item_Object_identifier
          ,Coalesce(SPO.Top,SPO.Right,SPO.Unknown,SPO.Bottom,SPO.Left) order by A.Object_identifier)
          ,'_'    
          ,Coalesce(SPO.Top,SPO.Right,SPO.Unknown,SPO.Bottom,SPO.Left)
          )
      Else Coalesce(SPO.Top,SPO.Right,SPO.Unknown,SPO.Bottom,SPO.Left) END as Marking
,A.Class
From Sigraph_Silver.S_ItemFunction as A 
-- Ignore the item id, where we have pins.
LEFT ANTI JOIN VW_Terminals_Prep_1 ELP ON ELP.database_name=A.database_name
and ELP.Item_Dynamic_Class=A.Item_dynamic_class and ELP.Item_Object_identifier=A.Item_object_identifier

Inner join sigraph_reference.Symbol_pin_orientation SPO ON 
SPO.Symbol_Name=Case When A.Item_Dynamic_Class<>'LC_PLC_Module' Then A.Symbol_Name END
and Coalesce(SPO.Top,SPO.Right,SPO.Unknown,SPO.Bottom,SPO.Left) is not null
-- Consider only those records, where we are getting pin orientation
Where A.Type in ('Device','FTA','Terminal Strip')


