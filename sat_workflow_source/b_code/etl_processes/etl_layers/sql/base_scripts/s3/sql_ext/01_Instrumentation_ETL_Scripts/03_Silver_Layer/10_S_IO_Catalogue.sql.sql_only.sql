
CREATE OR REPLACE TEMP VIEW VW_IOCard_Prep_Query
As

    Select Distinct
     A.database_name
    ,A.dynamic_class
    ,A.object_identifier
    ,A.Item_dynamic_class
    ,A.Item_object_identifier
    ,Case When B.ModelNo like '%10101/2/1%' Then '10101/2/1' Else B.ModelNo END as Model_Number
    ,CONCAT(Count(ChannelNumber) Over(Partition by A.Database_name,IOType,A.Item_Object_Identifier)
            ,' - Channel -',Description) as Description
    ,Case When B.ModelNo like '%10101/2/1%' Then 'HONEYWELL' 
          Else MAX(Manufacturer) Over(Partition by B.ModelNo) END as Manufacturer
    ,ChannelNumber
    ,IOType	
    ,A.Class
     --,Tag_Number
    --,Loop_Number
    --,Document_Number
    ,Count(ChannelNumber) Over(Partition by A.Database_name,IOType,A.Item_Object_Identifier) as NoOfPoints
    ,Dense_Rank() Over(order by B.ModelNo,IOType,ChannelNumber) as Model_Channel_Rank
    from Sigraph_silver.S_Itemfunction A

    Inner join Sigraph_silver.S_Item_Function_Model B
    On A.database_name=B.database_name
    and A.Item_Dynamic_Class=B.Item_Dynamic_Class
    and A.Item_Object_identifier=B.Item_Object_identifier
    and A.Dynamic_Class=B.Dynamic_Class
    and A.Object_Identifier=B.Object_Identifier

    Where Type='IO Module' 
and  Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1;

Create Or Replace Temp View VW_IO_TerminalMarking
As

Select 
     A.database_name
    ,A.dynamic_class
    ,A.object_identifier
    ,A.item_dynamic_class
    ,A.item_object_identifier
    ,A.IOType
    ,A.ChannelNumber
    ,'10101/2/1' as Model_Number
    ,ioModelTerminalMarking(B.ModelNo,A.ChannelNumber) as TerminalsPerMarking
    ,Count(1) over(partition by A.database_name    ,A.dynamic_class    ,A.object_identifier) as Terminal_Count
     
From Sigraph_Silver.S_ItemFunction as A 

Inner join Sigraph_silver.S_Item_Function_Model B
    On A.database_name=B.database_name
    and A.Item_Dynamic_Class=B.Item_Dynamic_Class
    and A.Item_Object_identifier=B.Item_Object_identifier
    and A.Dynamic_Class=B.Dynamic_Class
    and A.Object_Identifier=B.Object_Identifier

Where A.Type='IO Module'
   and  Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1
   and B.ModelNo like '%10101/2/1%' -- for this model, discipline shared the terminal markings, so extracting using function.

UNION All

Select 
     A.database_name
    ,A.dynamic_class
    ,A.object_identifier
    ,A.item_dynamic_class
    ,A.item_object_identifier
    ,A.IOType
    ,A.ChannelNumber
    ,B.ModelNo as Model_Number
    ,PM.Terminal_Marking as TerminalsPerMarking
    ,Count(1) over(partition by A.database_name    ,A.dynamic_class    ,A.object_identifier) as Terminal_Count
     
From Sigraph_Silver.S_ItemFunction as A 

Inner join Sigraph_silver.S_Item_Function_Model B
    On A.database_name=B.database_name
    and A.Item_Dynamic_Class=B.Item_Dynamic_Class
    and A.Item_Object_identifier=B.Item_Object_identifier
    and A.Dynamic_Class=B.Dynamic_Class
    and A.Object_Identifier=B.Object_Identifier

Inner join Sigraph_silver.S_Pin as PM 
On A.database_name=PM.database_name 
and A.Dynamic_Class=PM.Function_Dynamic_Class
and A.Object_Identifier=PM.Function_Object_Identifier
and PM.Pin_Type='EL_PIN'

Where A.Type='IO Module'
   and  Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1
   and B.ModelNo Not like '%10101/2/1%';

-- For same model and channel if for one IO card, we have 2 terminals and for few others we have only 1 terminal, then mark the 2 terminal marking from the IO Cards, where we already have 2 terminals.

CREATE OR REPLACE TEMP VIEW VW_IO_TerminalMarking_Prep
As
Select
A.database_name
    ,A.dynamic_class
    ,A.object_identifier
    ,A.item_dynamic_class
    ,A.item_object_identifier
    ,A.IOType
    ,A.ChannelNumber
    ,A.Model_Number
    ,A.TerminalsPerMarking
    ,A.Terminal_Count
from VW_IO_TerminalMarking A
Where Terminal_Count>=2

UNION

SELECT
A.database_name
    ,A.dynamic_class
    ,A.object_identifier
    ,A.item_dynamic_class
    ,A.item_object_identifier
    ,A.IOType
    ,A.ChannelNumber
    ,A.Model_Number
    ,Coalesce(B.TerminalsPerMarking,A.TerminalsPerMarking) as TerminalsPerMarking
    ,Coalesce(B.Terminal_Count,A.Terminal_Count) as Terminal_Count
From VW_IO_TerminalMarking A
Left outer join VW_IO_TerminalMarking B On A.database_name=B.database_name and A.Model_Number=B.model_number 
and A.ChannelNumber=B.ChannelNumber and B.Terminal_Count=2
Where A.Terminal_Count=1;

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
Where TerminalsPerMarking is not null;

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