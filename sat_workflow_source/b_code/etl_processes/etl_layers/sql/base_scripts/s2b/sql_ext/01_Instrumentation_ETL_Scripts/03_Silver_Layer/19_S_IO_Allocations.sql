

create or replace temp view IO_Allocations_Prep_Query
as
Select Distinct
 SR.database_name
,SR.object_identifier
,Coalesce(Ins.Loop_Number,A.Loop_Number) as Loop_Number
,Coalesce(Ins.Tag_Number,A.Tag_number) as Tag_Number
,SR.Location_Designation as Parent_Equipment_No
,SR.IOType
,SR.Product_Key as Equipment_No
,SR.CatalogueNo
,SR.ChannelNumber
,SR.Class
,A.From_Item
,A.To_Item
From (
Select 
Coalesce(I.Tag_Number,H.Tag_Number,G.Tag_Number,F.Tag_Number,E.Tag_Number
             ,D.Tag_Number,C.Tag_Number
             ,B.Tag_Number) as Tag_Number
,Coalesce(I.From_Item,H.From_Item,G.From_Item,F.From_Item,E.From_Item
             ,D.From_Item,C.From_Item
             ,B.From_Item) as From_Item             
,Coalesce(I.To_Item,H.To_Item,G.To_Item,F.To_Item,E.To_Item
             ,D.To_Item,C.To_Item
             ,B.To_Item) as To_Item        
,A.Loop_Number
,A.Database_name
,A.Object_identifier
,A.From_Object_identifier
,A.From_Dynamic_Class          
,A.From_Item_Dynamic_Class
,A.From_Item_Object_identifier
from sigraph_silver.S_Connection A
Left outer join sigraph_silver.S_Connection B ON A.database_name=B.database_name and A.To_Object_Identifier=B.From_Object_Identifier
Left outer join sigraph_silver.S_Connection C ON B.database_name=C.database_name and B.To_Object_Identifier=C.From_Object_Identifier
Left outer join sigraph_silver.S_Connection D ON C.database_name=D.database_name and C.To_Object_Identifier=D.From_Object_Identifier
Left outer join sigraph_silver.S_Connection E ON D.database_name=E.database_name and D.To_Object_Identifier=E.From_Object_Identifier
Left outer join sigraph_silver.S_Connection F ON E.database_name=F.database_name and E.To_Object_Identifier=F.From_Object_Identifier
Left outer join sigraph_silver.S_Connection G ON F.database_name=G.database_name and F.To_Object_Identifier=G.From_Object_Identifier
Left outer join sigraph_silver.S_Connection H ON G.database_name=H.database_name and G.To_Object_Identifier=H.From_Object_Identifier
Left outer join sigraph_silver.S_Connection I ON H.database_name=I.database_name and H.To_Object_Identifier=I.From_Object_Identifier

) as A
Inner join (
Select Distinct 
VF.database_name
,VF.Object_identifier
,VF.Dynamic_Class
,VF.ChannelNumber
,VF.item_slot as Slot 
,VF.Product_Key
,VF.Location_Designation
,VM.ModelNo as CatalogueNo
,VF.IOType	
,VF.Loop_Number
,VF.Class
from Sigraph_silver.S_Itemfunction VF
Inner join Sigraph_silver.S_Item_Function_Model VM On VF.database_name=VM.database_name 
and VF.Item_Dynamic_Class=VM.Item_Dynamic_Class
and VF.Item_Object_identifier=VM.Item_Object_identifier
and VF.Dynamic_Class=VM.Dynamic_Class
and VF.Object_Identifier=VM.Object_Identifier
Where Coalesce(item_slot,'')<>'' and Type='IO Module'
) as SR ON SR.database_name=A.Database_name 
and SR.Loop_Number=A.Loop_Number
and SR.Dynamic_Class=A.From_Dynamic_Class
and SR.Object_identifier=A.From_Object_Identifier 
Left outer join (
Select 
database_name
,Loop_Number
,Tag_Number
,Count(Tag_Number) Over(Partition by database_name,Loop_Number) as InstrumentCount
From (
Select Distinct database_name,Loop_Number,Tag_Number from  Sigraph_silver.S_Connection Where From_Location is null  
UNION
Select Distinct database_name,Loop_Number,Tag_Number from  Sigraph_silver.S_Connection Where To_Location is null  
) as A
) as Ins 
ON Ins.InstrumentCount=1 
and Ins.database_name=A.database_name 
and Ins.Loop_Number=A.Loop_Number



create or replace temp view IO_Allocations_Instrument
as
Select Distinct
 SR.database_name
,SR.object_identifier
,SR.Tag_Number
,SR.Parent_Equipment_No
,SR.IOType
,SR.Equipment_No
,SR.CatalogueNo
,SR.ChannelNumber
,SR.Class
From IO_Allocations_Prep_Query SR
LEFT SEMI JOIN Sigraph_silver.S_ItemFunction DP 
ON DP.database_name=SR.database_name 
and DP.Tag_Number=SR.Tag_Number
and DP.Type='Field Device'
Where SR.Tag_Number is not null


Select Distinct
 SR.*
From IO_Allocations_Prep_Query SR
LEFT ANTI JOIN Sigraph_silver.S_ItemFunction DP 
ON DP.database_name=SR.database_name 
and DP.Tag_Number=SR.Tag_Number
and DP.Type='Field Device'
Where SR.Loop_Number='0313H4960'


 df = spark.sql("select * from IO_Allocations where database_name in (Select * from VW_Database_names)")

 dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_IO_Allocations",True)

 df.write.save(
     path = f"dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_IO_Allocations"
    ,mode="overwrite"
    ,overwriteSchema = True
 )


Select Distinct
 SR.database_name
,SR.object_identifier
,Coalesce(Ins.Tag_Number,A.Tag_number) as Tag_Number
,SR.Location_Designation as Parent_Equipment_No
,SR.IOType
,SR.Product_Key as Equipment_No
,SR.CatalogueNo
,SR.ChannelNumber
,SR.Class
From (
Select 
Coalesce(I.Tag_Number,H.Tag_Number,G.Tag_Number,F.Tag_Number,E.Tag_Number
             ,D.Tag_Number,C.Tag_Number
             ,B.Tag_Number) as Tag_Number
,A.Loop_Number
,A.Database_name
,A.Object_identifier
,A.From_Object_identifier
,A.From_Dynamic_Class          
,A.From_Item_Dynamic_Class
,A.From_Item_Object_identifier
from sigraph_silver.S_Connection A
Left outer join sigraph_silver.S_Connection B ON A.database_name=B.database_name and A.To_Object_Identifier=B.From_Object_Identifier
Left outer join sigraph_silver.S_Connection C ON B.database_name=C.database_name and B.To_Object_Identifier=C.From_Object_Identifier
Left outer join sigraph_silver.S_Connection D ON C.database_name=D.database_name and C.To_Object_Identifier=D.From_Object_Identifier
Left outer join sigraph_silver.S_Connection E ON D.database_name=E.database_name and D.To_Object_Identifier=E.From_Object_Identifier
Left outer join sigraph_silver.S_Connection F ON E.database_name=F.database_name and E.To_Object_Identifier=F.From_Object_Identifier
Left outer join sigraph_silver.S_Connection G ON F.database_name=G.database_name and F.To_Object_Identifier=G.From_Object_Identifier
Left outer join sigraph_silver.S_Connection H ON G.database_name=H.database_name and G.To_Object_Identifier=H.From_Object_Identifier
Left outer join sigraph_silver.S_Connection I ON H.database_name=I.database_name and H.To_Object_Identifier=I.From_Object_Identifier

) as A
Inner join (
Select Distinct 
VF.database_name
,VF.Object_identifier
,VF.Dynamic_Class
,VF.ChannelNumber
,VF.item_slot as Slot 
,VF.Product_Key
,VF.Location_Designation
,VM.ModelNo as CatalogueNo
,VF.IOType	
,VF.Loop_Number
,VF.Class
from Sigraph_silver.S_Itemfunction VF
Inner join Sigraph_silver.S_Item_Function_Model VM On VF.database_name=VM.database_name 
and VF.Item_Dynamic_Class=VM.Item_Dynamic_Class
and VF.Item_Object_identifier=VM.Item_Object_identifier
and VF.Dynamic_Class=VM.Dynamic_Class
and VF.Object_Identifier=VM.Object_Identifier
Where Coalesce(item_slot,'')<>'' and Type='IO Module'
) as SR ON SR.database_name=A.Database_name 
and SR.Loop_Number=A.Loop_Number
and SR.Dynamic_Class=A.From_Dynamic_Class
and SR.Object_identifier=A.From_Object_Identifier 
LEFT ANTI JOIN Sigraph_silver.S_ItemFunction DP 
ON DP.database_name=A.database_name 
and DP.Tag_Number=Coalesce(Ins.Tag_Number,A.Tag_number) 
and DP.Type='Field Device'
Where A.Loop_Number='0313H4960'


Select * from Sigraph_Silver.S_Connection Where From_Item='HS4955'
union
Select * from Sigraph_Silver.S_Connection Where To_Item='HS4955'


Select *
from sigraph_silver.S_Connection A
Left outer join sigraph_silver.S_Connection B ON A.database_name=B.database_name and A.To_Object_Identifier=B.From_Object_Identifier
Where A.FROM_object_identifier='ID_28_c_1f59338'


Select * from sigraph_silver.S_Connection Where To_object_identifier='ID_28_c_1ea59a8'



