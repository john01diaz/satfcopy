
CREATE OR REPLACE TEMP VIEW VW_IO_CATALOGUE
AS

Select
A.database_name
,A.dynamic_class
,A.object_identifier
,A.Item_dynamic_class
,A.Item_object_identifier
,A.Model_Number
,A.Description
,A.Manufacturer
,'' as DescriptionDrawing
,'' as Channel
,'True' as AllowUse	
,A.IOType
,A.NoOfPoints
,A.TerminalsPerPointChannel	
,Unique_ID
,Case When A.Unique_ID<>1 Then Concat(A.ChannelNumber,'-',A.TerminalsPerMarking ) 
      Else A.TerminalsPerMarking
      End as TerminalsPerMarking
,A.Class
,A.ChannelNumber as ChannelNumber
,A.Catalogue_RNT
From (
Select
A.database_name
,A.dynamic_class
,A.object_identifier
,A.Item_dynamic_class
,A.Item_object_identifier
,A.Model_Number
,A.Description
,A.Manufacturer
,'' as DescriptionDrawing
,'' as Channel
,'True' as AllowUse	
,A.IOType
,A.NoOfPoints
,Case When TerminalsPerPointChannel>2 Then TerminalsPerPointChannel Else 2 END as TerminalsPerPointChannel	
,B.TerminalsPerMarking 
,A.Class
,A.ChannelNumber as ChannelNumber
--,Dense_Rank() Over(Partition by A.Model_Number,A.NoOfPoints,B.TerminalsPerMarking 
  --            order by A.Item_object_identifier,A.object_identifier) as Catalogue_RNT
,Dense_Rank() Over(Partition by A.Model_Number,A.NoOfPoints  order by A.Item_object_identifier) as Catalogue_RNT
,Dense_Rank() Over(Partition by A.Model_Number,A.IOType,B.TerminalsPerMarking 
              order by A.Model_Number,A.IOType,A.ChannelNumber) as Unique_ID
From VW_IOCard_Prep_Query A
Inner join (
Select 
 A.database_name
,A.dynamic_class
,A.object_identifier
,A.item_dynamic_class
,A.item_object_identifier
,A.IOType
,A.TerminalsPerMarking
,A.Terminal_Count 
,MAX(Coalesce(Terminal_Count,0)) Over(Partition by database_name,Item_object_identifier,IOType) as TerminalsPerPointChannel
from VW_IO_TerminalMarking_Prep A
UNION
Select 
 A.database_name
,A.dynamic_class
,A.object_identifier
,A.item_dynamic_class
,A.item_object_identifier
,A.IOType
,A.TerminalsPerMarking
,A.Terminal_Count 
,MAX(Coalesce(Terminal_Count,0)) Over(Partition by database_name,Item_object_identifier,IOType) as TerminalsPerPointChannel
from VW_IO_Channel_TerminalMarking A
)as B ON A.database_name=B.database_name and A.dynamic_Class=B.dynamic_Class
and A.Object_identifier=B.Object_identifier
) as A
Order by A.Model_Number,A.NoOfPoints,Cast(A.ChannelNumber as Bigint)

