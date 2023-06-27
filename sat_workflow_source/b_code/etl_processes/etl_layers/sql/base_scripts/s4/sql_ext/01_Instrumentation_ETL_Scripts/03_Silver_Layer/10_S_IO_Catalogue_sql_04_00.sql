
Create Or Replace Temp View VW_IO_Channel_TerminalMarking
As

-- Scenari 1 : If we dont have any electrical pin for given Item Object identifier, then add channel + and channel - as terminal marking

Select Distinct
 A.database_name
,A.dynamic_class
,A.object_identifier
,A.item_dynamic_class
,A.item_object_identifier
,A.IOType
,Concat(A.ChannelNumber,'+')  as TerminalsPerMarking
,0 as Terminal_Count
from sigraph_silver.S_Itemfunction A
Left ANTI join VW_IO_TerminalMarking_Prep B On A.database_name=B.database_name and A.Item_dynamic_class=B.Item_dynamic_class
and A.Item_object_identifier=B.Item_Object_identifier
Where Type='IO Module' and A.ChannelNumber is not null and A.ChannelNumber<>'' and A.ChannelNumber<>'0'
and Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1

union

Select Distinct
 A.database_name
,A.dynamic_class
,A.object_identifier
,A.item_dynamic_class
,A.item_object_identifier
,A.IOType
,Concat(A.ChannelNumber,'-')  as TerminalsPerMarking
,0 as Terminal_Count
from sigraph_silver.S_Itemfunction A
Left ANTI join VW_IO_TerminalMarking_Prep B On A.database_name=B.database_name and A.Item_dynamic_class=B.Item_dynamic_class
and A.Item_object_identifier=B.Item_Object_identifier
Where Type='IO Module' and A.ChannelNumber is not null and A.ChannelNumber<>'' and A.ChannelNumber<>'0'
and Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1

-- Scenario 2 : If we have electrical pin, then if few channel has electrical pin marking and few dont. Then for those which has 1 pin, add another terminal marking like Not Exists 1, Not Exists 2 .. For those channels where we dont have electrical pin. Add like AA EMPTY, AB EMPTY etc... 
UNION

Select Distinct
 A.database_name
,A.dynamic_class
,A.object_identifier
,A.item_dynamic_class
,A.item_object_identifier
,A.IOType
,Concat('Not Exists', 
                Row_Number() Over(Partition by A.database_name,A.Item_Object_Identifier 
                            order by Cast(Translate(A.ChannelNumber,'AI_,AO_,DI_,DO_,FIELD BUS_','') as Bigint),A.object_identifier))
     as TerminalsPerMarking
  ,Coalesce(C.Terminal_Count,0) as Terminal_Count
from sigraph_silver.S_Itemfunction A

LEFT SEMI JOIN (
    Select Distinct database_name,Item_dynamic_class,Item_Object_identifier
    From VW_IO_TerminalMarking_Prep
 ) as B On A.database_name=B.database_name and A.Item_dynamic_class=B.Item_dynamic_class
and A.Item_object_identifier=B.Item_Object_identifier

Inner join VW_IO_TerminalMarking_Prep C On A.database_name=C.database_name and A.dynamic_class=C.dynamic_class
and A.object_identifier=C.Object_identifier and C.Terminal_Count=1

Where Type='IO Module' and A.ChannelNumber is not null and A.ChannelNumber<>'' and A.ChannelNumber<>'0'
and Case When Type='IO Module' and (Coalesce(A.ChannelNumber,'')='' OR A.ChannelNumber='0') Then 0 Else 1 End=1

union

Select 
 A.database_name
,A.dynamic_class
,A.object_identifier
,A.item_dynamic_class
,A.item_object_identifier
,A.IOType
,explode(split(TerminalsPerMarking,',')) as TerminalsPerMarking
,2 as Terminal_Count
From (
Select Distinct
 A.database_name
,A.dynamic_class
,A.object_identifier
,A.item_dynamic_class
,A.item_object_identifier
,A.IOType
,ioTerminalMarking(D.Model_Channel_Rank) as TerminalsPerMarking
from sigraph_silver.S_Itemfunction A

-- Get the unique number for each model number, channle numbr and IOType and based on this derive the terminal marking, As AA,EMPTY, AB EMPTY etc..
INNER join
(
Select Distinct
database_name
,A.dynamic_class
,A.object_identifier
,Dense_Rank() Over(order by Model_Number,IOType,ChannelNumber) as Model_Channel_Rank
From VW_IOCard_Prep_Query A
LEFT SEMI JOIN (
    Select Distinct database_name,Item_dynamic_class,Item_Object_identifier
    From VW_IO_TerminalMarking_Prep
 ) as B On A.database_name=B.database_name and A.Item_dynamic_class=B.Item_dynamic_class
and A.Item_object_identifier=B.Item_Object_identifier
Left ANTI join VW_IO_TerminalMarking_Prep C On A.database_name=C.database_name and A.dynamic_class=C.dynamic_class
and A.object_identifier=C.Object_identifier 
) as D On A.database_name=D.database_name and A.dynamic_class=D.dynamic_class
and A.object_identifier=D.Object_identifier 

Where Type='IO Module' and A.ChannelNumber is not null and A.ChannelNumber<>'' and A.ChannelNumber<>'0'
and Case When Type='IO Module' and (Coalesce(A.ChannelNumber,'')='' OR A.ChannelNumber='0') Then 0 Else 1 End=1
) as A
Where TerminalsPerMarking is not null



