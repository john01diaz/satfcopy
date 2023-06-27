


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




Create OR Replace Temp View VW_Terminals_Prep_2
As

Select 
 A.database_name
,A.dynamic_class
,A.object_identifier
,A.Item_Dynamic_Class
,A.Item_object_Identifier
,Coalesce(Location_Designation,'') as Parent_Equipment_No
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
LEFT ANTI JOIN VW_Terminals_Prep_1 ELP ON ELP.database_name=A.database_name
and ELP.Item_Dynamic_Class=A.Item_dynamic_class and ELP.Item_Object_identifier=A.Item_object_identifier

Inner join sigraph_reference.Symbol_pin_orientation SPO ON 
SPO.Symbol_Name=Case When A.Item_Dynamic_Class<>'LC_PLC_Module' Then A.Symbol_Name END
and Coalesce(SPO.Top,SPO.Right,SPO.Unknown,SPO.Bottom,SPO.Left) is not null
Where A.Type in ('Device','FTA','Terminal Strip')



Create Or Replace Temp View VW_Terminals_Prep_3
As
Select
 A.database_name
,A.dynamic_class
,A.object_identifier
,A.Item_Dynamic_Class
,A.Item_object_Identifier
,Coalesce(A.Location_Designation,'') as Parent_Equipment_No
,A.Product_Key as Equipment_No
,B.TerminalsPerMarking as Marking
,A.Class
from sigraph_silver.S_Itemfunction A
Inner join sigraph_silver.S_IO_Catalogue B On A.database_name=B.database_name and A.dynamic_class=B.dynamic_class
and A.object_identifier=B.object_identifier



Create or Replace Temp View VW_Terminals_Prep_4
As
select A.*
From (
Select
 A.database_name
,A.dynamic_class
,A.object_identifier
,A.Item_Dynamic_Class
,A.Item_object_Identifier
,Coalesce(A.Location_Designation,'') as Parent_Equipment_No
,A.Product_Key as Equipment_No
,From_Terminal_Marking as Marking
,A.Class
from sigraph_silver.S_Itemfunction A
Inner join sigraph_silver.S_Connection  B On A.database_name=B.database_name and A.dynamic_class=B.from_dynamic_class
and A.object_identifier=B.From_object_identifier
Where A.Type='FTA'
union
Select
 A.database_name
,A.dynamic_class
,A.object_identifier
,A.Item_Dynamic_Class
,A.Item_object_Identifier
,Coalesce(A.Location_Designation,'') as Parent_Equipment_No
,A.Product_Key as Equipment_No
,To_Terminal_Marking as Marking
,A.Class
from sigraph_silver.S_Itemfunction A
Inner join sigraph_silver.S_Connection  B On A.database_name=B.database_name and A.dynamic_class=B.To_dynamic_class
and A.object_identifier=B.To_object_identifier
) as A
left anti join (
Select * from VW_Terminals_Prep_1
UNION
Select * from VW_Terminals_Prep_2
UNION
Select * from VW_Terminals_Prep_3
) as B On A.database_name=B.database_name and A.dynamic_class=B.dynamic_class
and A.object_identifier=B.object_identifier and A.Marking=B.Marking




Create Or Replace Temp View VW_Terminals
As
Select
database_name
,dynamic_class
,object_identifier
,Parent_Equipment_No
,Equipment_No
,Marking
,Dense_Rank() Over(Partition by Parent_Equipment_No,Equipment_No order by Cast(Marking as Bigint),Marking) as Sequence
,Class
From (
Select * from VW_Terminals_Prep_1
UNION
Select * from VW_Terminals_Prep_2
UNION
Select * from VW_Terminals_Prep_3
UNION
Select * from VW_Terminals_Prep_4
) as A
LEFT SEMI JOIN (
Select 
TagNo as Equipment_No 
from Sigraph_Silver.S_Instrument_Index
UNION
Select Equipment_No 
from Sigraph_Silver.S_Component
) as RD 
ON RD.Equipment_No=A.Equipment_No


 dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Terminals", True)

 DF=spark.sql('Select * from VW_Terminals where database_name in (Select * from VW_Database_names)')

 DF = cleansing_df(DF)

 DF.write.save(
      path        = 'dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Terminals'
     ,format      = "delta"
     ,mode        = "overwrite"
     ,overwriteSchema = True
 )
