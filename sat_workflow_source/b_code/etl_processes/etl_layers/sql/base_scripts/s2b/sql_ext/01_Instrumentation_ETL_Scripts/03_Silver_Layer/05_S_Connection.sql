


 Create Or Replace TEMP VIEW VW_Loop_Tag_DocumentNumber
 As

 Select
 L.loop_database_name
 ,L.CS_loop_element_id as Tag_Number
 ,L.CS_Loop_ID as Loop_Number
 ,DM_Document_Number as Document_Number
 ,L.LC_Item_function_CS_Loop_element_href
 ,L.LC_Item_function_CS_Loop_element_dyn_class
 ,l.CS_manufacturer_mce
 ,l.CS_device_type
 ,Case when loop_dynamic_class='CS_Loop_spez' Then 'Instrumentation'
       when loop_dynamic_class='CS_Electrical_load' Then 'Electrical'
       END as Class
 From sigraph.CS_Layer_Loop_Loop_elements L 
 left outer join (
 Select
 DM_Document_Number
 ,database_name
 ,CS_Loop_DM_Document_dyn_class
 ,CS_Loop_DM_Document_href
 from sigraph.DM_Circuit_diagram 
 qualify Row_Number() over(Partition by database_name,CS_Loop_DM_Document_href order by Cast(DM_modification_time as timestamp) desc) =1
 ) B 
 ON  L.loop_database_name=B.database_name 
 and L.loop_dynamic_class=B.CS_Loop_DM_Document_dyn_class
 and L.loop_object_identifier=B.CS_Loop_DM_Document_href


Create Or Replace Temp View VW_Connection_Pin_Unions
As
Select Distinct
FCon.object_identifier
,FCon.database_name
,From_Pin_Object_Identifier
,From_Pin_Dynamic_Class
,To_Pin_Object_Identifier
,To_Pin_Dynamic_Class
from (
Select 
Con.object_identifier
,Con.database_name
,Coalesce(Con.From_connection_pin_href,WF.from_LC_Wire_function_Function_pin_href) as From_Pin_Object_Identifier
,Coalesce(Con.from_connection_pin_dyn_class,WF.from_LC_Wire_function_Function_pin_dyn_class) as From_Pin_Dynamic_Class
From Sigraph.Connection Con
Left outer join sigraph.Wire_function WF 
ON WF.database_name=Con.database_name 
and WF.LC_wire_connection_rel_href = Con.object_identifier
Where Coalesce(Con.From_connection_pin_href,WF.from_LC_Wire_function_Function_pin_href) is not null
) as FCon
Inner join (
Select 
Con.object_identifier
,Con.database_name
,Coalesce(Con.To_connection_pin_href,WF.to_LC_Wire_function_Function_pin_href) as To_Pin_Object_Identifier
,Coalesce(Con.To_connection_pin_dyn_class,WF.To_LC_Wire_function_Function_pin_dyn_class) as To_Pin_Dynamic_Class
From Sigraph.Connection Con
Left outer join sigraph.Wire_function WF 
ON WF.database_name=Con.database_name 
and WF.LC_wire_connection_rel_href = Con.object_identifier
Where Coalesce(Con.To_connection_pin_href,WF.to_LC_Wire_function_Function_pin_href) is not null
) as TCon On FCon.database_name=TCon.Database_name and FCon.Object_Identifier=TCon.Object_Identifier


Create Or Replace Temp View Vw_Connection_Pin_Extract_1
As
Select * 
from (
Select
Con.object_identifier
,Con.database_name
,MAX(RPN.Dynamic_Class) as From_Pin_Dynamic_Class
,MAX(RPN.Object_Identifier) as From_Pin_Object_Identifier
,MAX(LPN.Dynamic_Class) as To_Pin_Dynamic_Class
,MAX(LPN.Object_Identifier) as To_Pin_Object_Identifier
From (
Select 
object_identifier
,database_name
,From_Pin_Object_Identifier as Pin_Object_Identifier
,From_Pin_Dynamic_Class as Pin_Dynamic_Class
From VW_Connection_Pin_Unions
UNION
Select 
object_identifier
,database_name
,To_Pin_Object_Identifier as Pin_Object_Identifier
,To_Pin_Dynamic_Class as Pin_Dynamic_Class
From VW_Connection_Pin_Unions
) as Con
Left outer join sigraph_silver.S_Pin RPN ON RPN.database_name=Con.database_name 
and RPN.Object_Identifier=Con.Pin_Object_Identifier and RPN.Dynamic_Class=Con.Pin_Dynamic_Class
and Coalesce(RPN.Terminal_Side,'Left')='Right'
Left outer join sigraph_silver.S_Pin LPN ON LPN.database_name=Con.database_name 
and LPN.Object_Identifier=Con.Pin_Object_Identifier and LPN.Dynamic_Class=Con.Pin_Dynamic_Class
and LPN.Terminal_Side='Left'
Where Coalesce(RPN.Object_Identifier,LPN.Object_Identifier) is not null
Group by Con.object_identifier,Con.database_name
) as A Where  From_Pin_Object_Identifier is not null and To_Pin_Object_Identifier  is not null


CREATE OR REPLACE TEMP VIEW Vw_Connection_Pin_Extract_2
As
Select
A.object_identifier
,A.database_name
,A.From_Pin_Dynamic_Class
,A.From_Pin_Object_Identifier
,A.To_Pin_Dynamic_Class
,A.To_Pin_Object_Identifier
from VW_Connection_Pin_Unions A
Left Anti Join Vw_Connection_Pin_Extract_1 B On A.database_name=B.Database_name and A.Object_identifier=B.Object_Identifier



CREATE OR REPLACE TEMP VIEW VW_Connection_Prep_Query_Non_IO
As
Select Distinct
Con.object_identifier
,Con.database_name
,RPN.Dynamic_Class as From_Pin_Dynamic_Class
,RPN.Object_Identifier as From_Pin_Object_Identifier
,LPN.Dynamic_Class as To_Pin_Dynamic_Class
,LPN.Object_Identifier as To_Pin_Object_Identifier
,FR.dynamic_Class as FROM_dynamic_Class
,FR.object_identifier as FROM_object_identifier
,FR.Item_dynamic_Class as FROM_Item_dynamic_Class
,FR.Item_object_identifier as FROM_Item_object_identifier
,To.dynamic_Class as To_dynamic_Class
,To.object_identifier as To_object_identifier
,To.Item_dynamic_Class as To_Item_dynamic_Class
,To.Item_object_identifier as To_Item_object_identifier
,C.Object_Identifier as Cable_Object_Identifier
,WF.Object_Identifier as Wire_Object_Identifier
,FR.location_designation as From_Location
,FR.facility_designation as From_Facility
,FR.product_Key as From_Item
,RPN.Terminal_Marking as From_Terminal_Marking
,C.LC_item_product_des as Cable
,WF.LC_wire_function_x_section as Wire_Cross_Section
,DCC.Core_Markings as Wire_Markings
,To.location_designation as To_Location
,To.facility_designation as To_Facility 
,To.product_Key as To_Item
,LPN.Terminal_Marking as To_Terminal_Marking
,Coalesce(FR.Loop_Number,To.Loop_Number) as Loop_Number
,Coalesce(FR.Tag_Number,To.Tag_Number) as Tag_Number
,Coalesce(FR.Document_Number,To.Document_Number) as Document_Number

From (
Select * from Vw_Connection_Pin_Extract_1
UNION
Select * from Vw_Connection_Pin_Extract_2
) Con
Left outer join sigraph_silver.S_Pin RPN ON RPN.database_name=Con.database_name 
and RPN.Dynamic_Class=Con.From_Pin_Dynamic_Class
and RPN.Object_Identifier=Con.From_Pin_Object_Identifier 

Left outer join sigraph_silver.S_ItemFunction as Fr 
On FR.database_name=RPN.database_name 
and FR.Dynamic_Class=RPN.Function_Dynamic_Class
and FR.Object_Identifier=RPN.Function_Object_Identifier

Left outer join sigraph_silver.S_Pin LPN ON LPN.database_name=Con.database_name 
and LPN.Dynamic_Class=Con.To_Pin_Dynamic_Class
and LPN.Object_Identifier=Con.To_Pin_Object_Identifier 

LEFT OUTER JOIN sigraph_silver.S_ItemFunction as To 
On To.database_name=LPN.database_name 
and To.Dynamic_Class=LPN.Function_Dynamic_Class
and To.Object_Identifier=LPN.Function_Object_Identifier
LEFT OUTER join sigraph.Wire_function WF 
ON WF.database_name=Con.database_name 
and WF.LC_wire_connection_rel_href = Con.object_identifier

LEFT OUTER join sigraph.Cable C 
ON WF.database_name=C.database_name 
and WF.LC_item_functions_rel_href = C.object_identifier

Left outer join sigraph_silver.S_CableCoreCatalogue DCC 
ON DCC.database_name=WF.database_name 
and DCC.object_Identifier=WF.Object_identifier



CREATE OR REPLACE TEMP VIEW VW_IO_Card_To_FTA_Connection_Query_1
As
Select
IO.database_name
,IO.Object_identifier as IO_Object_identifier
,IO.Dynamic_Class as IO_Dynamic_Class
,FTA.ObjecT_Identifier as FTA_ObjecT_Identifier
,FTA.Dynamic_Class as FTA_Dynamic_Class
,IO.Loop_Number
from sigraph_silver.S_ItemFunction IO
Inner join sigraph_silver.S_ItemFunction FTA 
ON IO.database_name=FTA.database_name
and IO.Loop_Number=FTA.Loop_Number
and IO.IOType=FTA.IOType
and IO.Channelnumber=FTA.ChannelNumber
and IO.Item_Slot=FTA.Item_Slot
Where IO.Type='IO Module' and FTA.Type='FTA'




CREATE OR REPLACE TEMP VIEW VW_IO_Card_To_FTA_Connection_Query_2
AS
Select
IO.database_name
,IO.Object_identifier as IO_Object_identifier
,IO.Dynamic_Class as IO_Dynamic_Class
,FTA.ObjecT_Identifier as FTA_ObjecT_Identifier
,FTA.Dynamic_Class as FTA_Dynamic_Class
,IO.Loop_Number
from sigraph_silver.S_ItemFunction IO
Inner join sigraph_silver.S_ItemFunction FTA 
ON IO.database_name=FTA.database_name
and IO.Loop_Number=FTA.Loop_Number
and IO.IOType=FTA.IOType
and IO.Channelnumber=FTA.ChannelNumber
LEFT ANTI JOIN VW_IO_Card_To_FTA_Connection_Query_1 CTE ON CTE.database_name=IO.database_name 
and CTE.IO_Dynamic_Class=IO.Dynamic_Class
and CTE.IO_Object_identifier=IO.Object_identifier
Where IO.Type='IO Module' and FTA.Type='FTA'



Create Or Replace Temp View VW_IO_Card_To_FTA_Connection_Query_3
As
Select
IO.database_name
,IO.Object_identifier as IO_Object_identifier
,IO.Dynamic_Class as IO_Dynamic_Class
,FTA.ObjecT_Identifier as FTA_ObjecT_Identifier
,FTA.Dynamic_Class as FTA_Dynamic_Class
,IO.Loop_Number
from (
Select *,Count(1) Over(Partition by IO.Database_name,IO.Loop_Number) as IO_Count
from sigraph_silver.S_ItemFunction IO
Where IO.Type='IO Module'
) as IO

Inner join sigraph_silver.S_ItemFunction FTA 
ON IO.database_name=FTA.database_name
and IO.Loop_Number=FTA.Loop_Number
and FTA.Type='FTA'
LEFT ANTI JOIN VW_IO_Card_To_FTA_Connection_Query_1 IFT1 ON IFT1.Database_name=IO.database_name 
and IFT1.IO_Dynamic_Class=IO.Dynamic_Class
and IFT1.IO_Object_identifier=IO.Object_Identifier
LEFT ANTI JOIN VW_IO_Card_To_FTA_Connection_Query_2 IFT2 ON IFT2.Database_name=IO.database_name 
and IFT2.IO_Dynamic_Class=IO.Dynamic_Class
and IFT2.IO_Object_identifier=IO.Object_Identifier
Where IO.IO_Count=1 -- Make sure we have only one IO Card for given loop



Create or Replace Temp View VW_IO_Card_To_FTA_Connection
As 
Select * from VW_IO_Card_To_FTA_Connection_Query_1
UNION
Select * from VW_IO_Card_To_FTA_Connection_Query_2
UNION
Select * from VW_IO_Card_To_FTA_Connection_Query_3


Create or Replace Temp view VW_Connection_Prep_Query_IO
AS
Select Distinct
IO.object_identifier
,IO.database_name
,'' as From_Pin_Dynamic_Class
,'' as From_Pin_Object_Identifier
,'' as To_Pin_Dynamic_Class
,'' as To_Pin_Object_Identifier
,IO.dynamic_Class as FROM_dynamic_Class
,IO.object_identifier as FROM_object_identifier
,IO.Item_dynamic_Class as FROM_Item_dynamic_Class
,IO.Item_object_identifier as FROM_Item_object_identifier
,FTA.dynamic_Class as To_dynamic_Class
,FTA.object_identifier as To_object_identifier
,FTA.Item_dynamic_Class as To_Item_dynamic_Class
,FTA.Item_object_identifier as To_Item_object_identifier
,'' as Cable_Object_Identifier
,'' as Wire_Object_Identifier
,IO.location_designation as From_Location
,IO.facility_designation as From_Facility
,IO.product_Key as From_Item
,Concat(IO.ChannelNumber,'+') as From_Terminal_Marking
,IO.Object_Identifier as Cable
,0 as Wire_Cross_Section
,1 as Wire_Markings
,FTA.location_designation as To_Location
,FTA.facility_designation as To_Facility 
,FTA.product_Key as To_Item
,Concat(FTA.ChannelNumber,'+') as To_Terminal_Marking
,Coalesce(IO.Loop_Number,FTA.Loop_Number) as Loop_Number
,Coalesce(IO.Tag_Number,FTA.Tag_Number) as Tag_Number
,Coalesce(IO.Document_Number,FTA.Document_Number) as Document_Number
from VW_IO_Card_To_FTA_Connection ConIO
Inner join sigraph_silver.S_ItemFunction IO 
ON ConIO.database_name=IO.Database_name
and ConIO.IO_Dynamic_Class=IO.Dynamic_Class
and ConIO.IO_Object_Identifier=IO.Object_Identifier
Inner join sigraph_silver.S_ItemFunction FTA
ON ConIO.database_name=IO.Database_name
and ConIO.FTA_Dynamic_Class=FTA.Dynamic_Class
and ConIO.FTA_Object_Identifier=FTA.Object_Identifier





Create Or Replace Temp View VW_Connection_Prep_Query
As
Select * from VW_Connection_Prep_Query_Non_IO Where From_dynamic_Class is not null and To_dynamic_Class is not null
UNION
Select * from VW_Connection_Prep_Query_IO     Where From_dynamic_Class is not null and To_dynamic_Class is not null


Create Or Replace TEMP View VW_Component_Function_Loop
As
Select 
A.database_name
,A.dynamic_Class
,A.object_identifier
,A.Tag_Number as Tag_Number 
,A.Loop_Number as Loop_Number
,A.Document_Number as Document_Number
,Row_Number() Over(Partition by A.database_name,object_identifier order by Loop_Number) as RNT
From (
Select Distinct
Con.database_name
,Con.To_dynamic_Class as dynamic_Class
,Con.To_object_identifier as object_identifier
,LT.Tag_Number as Tag_Number 
,LT.Loop_Number as Loop_Number
,LT.Document_Number as Document_Number
From VW_Connection_Prep_Query Con
Inner join VW_Loop_Tag_DocumentNumber LT 
ON LT.loop_database_name=Con.database_name 
and LT.LC_Item_function_CS_Loop_element_dyn_class=Con.From_dynamic_Class
and LT.LC_Item_function_CS_Loop_element_href=Con.From_object_identifier
Where To_dynamic_Class='LC_Component_function'
UNION
Select Distinct
Con.database_name
,Con.From_dynamic_Class
,Con.From_object_identifier
,LT.Tag_Number as Tag_Number 
,LT.Loop_Number as Loop_Number
,LT.Document_Number as Document_Number
From VW_Connection_Prep_Query Con
Inner join VW_Loop_Tag_DocumentNumber LT 
ON LT.loop_database_name=Con.database_name 
and LT.LC_Item_function_CS_Loop_element_dyn_class=Con.To_dynamic_Class
and LT.LC_Item_function_CS_Loop_element_href=Con.To_object_identifier
Where From_dynamic_Class='LC_Component_function'
) as A


Create Or Replace TEMP VIEW VW_Connection_Final_Extract
AS
Select Distinct
Con.object_identifier
,con.database_name
,Con.From_Pin_Object_Identifier as from_connection_pin_href
,Con.To_Pin_Object_Identifier as To_connection_pin_href
,Con.FROM_dynamic_Class
,Con.FROM_object_identifier
,Con.FROM_Item_dynamic_Class
,Con.FROM_Item_object_identifier
,Con.To_dynamic_Class
,Con.To_object_identifier
,Con.To_Item_dynamic_Class
,Con.To_Item_object_identifier
,Con.Cable_Object_Identifier
,Con.Wire_Object_Identifier
,Con.From_Location
,Con.From_Facility
,Con.From_Item
,Con.From_Terminal_Marking
,Coalesce(Con.Cable,Concat(Replace(Con.database_name,'_2016R3',''),'_',Con.Object_Identifier)) as Cable
,Coalesce(Con.Wire_Cross_Section,1) as Wire_Cross_Section
,Coalesce(Con.Wire_Markings,1) as Wire_Markings
,Con.To_Location
,Con.To_Facility 
,Con.To_Item
,Con.To_Terminal_Marking
,Coalesce(Con.Loop_Number,CFL.Loop_Number,CFLT.Loop_Number) as Loop_Number
,Con.Tag_Number
,Coalesce(Con.Document_Number,CFL.Document_Number,CFLT.Document_Number) as Document_Number
,Case  
      when Con.Wire_Markings is null and 
      Con.From_Item_Dynamic_class=Con.To_Item_Dynamic_class 
      and Con.From_Item_Object_Identifier=Con.To_Item_Object_Identifier
      and Con.From_Item_Dynamic_class='LC_Terminal_strip' 
      and Con.From_Dynamic_class=Con.To_Dynamic_Class and Con.From_Object_Identifier<>Con.To_Object_Identifier
      Then 'Link'
      when Con.Wire_Markings is not null and 
      Con.From_Item_Dynamic_class=Con.To_Item_Dynamic_class 
      and Con.From_Item_Object_Identifier=Con.To_Item_Object_Identifier
      and Con.From_Item_Dynamic_class='LC_Terminal_strip' 
      and Con.From_Dynamic_class=Con.To_Dynamic_Class and Con.From_Object_Identifier<>Con.To_Object_Identifier
      Then 'Loop'
      Else 'Wire'
      END as Connection_Type
From VW_Connection_Prep_Query Con
Left outer join VW_Component_Function_Loop CFL 
ON CFL.RNT=1 and CFL.database_name=Con.database_name 
and CFL.dynamic_Class=Con.From_dynamic_Class 
and CFL.object_identifier=Con.From_object_identifier
Left outer join VW_Component_Function_Loop CFLT 
ON CFLT.RNT=1 
and CFLT.database_name=Con.database_name 
and CFLT.dynamic_Class=Con.To_dynamic_Class and CFLT.object_identifier=Con.To_object_identifier




 DF=spark.sql('Select * from VW_Connection_Final_Extract where database_name == "R_2016R3"')

 dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Connection",True)

 DF.write.save(
      format = "delta"
     ,mode   = "overwrite"
     ,path   = "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Connection"
     ,overwriteSchema = True
 )
