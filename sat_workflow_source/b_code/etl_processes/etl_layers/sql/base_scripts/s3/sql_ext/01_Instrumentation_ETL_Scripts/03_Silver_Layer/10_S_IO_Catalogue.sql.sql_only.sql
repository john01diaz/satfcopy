
CREATE OR REPLACE TEMP VIEW VW_IOCard_Prep_Query
As

    Select Distinct
     A.database_name
    ,A.dynamic_class
    ,A.object_identifier
    ,B.ModelNo as Model_Number
    ,CONCAT(Count(ChannelNumber) Over(Partition by A.Database_name,IOType,A.Item_Object_Identifier)
            ,' - Channel -',Description) as Description
    ,MAX(Manufacturer) Over(Partition by B.ModelNo) as Manufacturer
    ,ChannelNumber
    ,IOType	
    ,A.Class
     --,Tag_Number
    --,Loop_Number
    --,Document_Number
    ,Count(ChannelNumber) Over(Partition by A.Database_name,IOType,A.Item_Object_Identifier) as NoOfPoints
    from Sigraph_silver.S_Itemfunction A

    Inner join Sigraph_silver.S_Item_Function_Model B
    On A.database_name=B.database_name
    and A.Item_Dynamic_Class=B.Item_Dynamic_Class
    and A.Item_Object_identifier=B.Item_Object_identifier
    and A.Dynamic_Class=B.Dynamic_Class
    and A.Object_Identifier=B.Object_Identifier

    Where Type='IO Module' 
    and ChannelNumber is not null 
    and ChannelNumber<>'0'
    and ChannelNumber<>'';

Create Or Replace Temp View VW_IO_TerminalMarking
As
Select Distinct
     A.database_name
    ,A.dynamic_class
    ,A.object_identifier
    ,PM.Terminal_Marking as TerminalsPerMarking
From Sigraph_Silver.S_ItemFunction as A 
Inner join Sigraph_silver.S_Pin as PM 
On A.database_name=PM.database_name 
and A.Dynamic_Class=PM.Function_Dynamic_Class
and A.Object_Identifier=PM.Function_Object_Identifier
and PM.Pin_Type='EL_PIN'
Where A.Type='IO Module'

UNION

Select
 A.database_name
,A.dynamic_class
,A.object_identifier
,Concat(A.ChannelNumber,'+')  as TerminalsPerMarking
from sigraph_silver.S_Itemfunction A
Where Type='IO Module' and A.ChannelNumber is not null and A.ChannelNumber<>'' and A.ChannelNumber<>'0'
and Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1

UNION
-- Get the terminals for IO, by using Channel Number + and -

Select 
 A.database_name
,A.dynamic_class
,A.object_identifier
,Concat(A.ChannelNumber,'-')  as TerminalsPerMarking
from sigraph_silver.S_Itemfunction A
Where Type='IO Module' and A.ChannelNumber is not null and A.ChannelNumber<>'' and A.ChannelNumber<>'0'
and Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1;

CREATE OR REPLACE TEMP VIEW VW_IO_CATALOGUE
AS
Select
A.database_name
,A.dynamic_class
,A.object_identifier
,A.Model_Number
,A.Description
,A.Manufacturer
,'' as DescriptionDrawing
,'' as Channel
,'True' as AllowUse	
,A.IOType
,A.NoOfPoints
,2 as TerminalsPerPointChannel	
,B.TerminalsPerMarking
,A.Class
,Row_Number() Over(Partition by A.Model_Number,A.NoOfPoints,B.TerminalsPerMarking 
              order by A.object_identifier) as Catalogue_RNT
From VW_IOCard_Prep_Query A
Inner join VW_IO_TerminalMarking B ON A.database_name=B.database_name and A.dynamic_Class=B.dynamic_Class
and A.Object_identifier=B.Object_identifier
Order by A.Model_Number,A.NoOfPoints,Cast(A.ChannelNumber as Bigint)