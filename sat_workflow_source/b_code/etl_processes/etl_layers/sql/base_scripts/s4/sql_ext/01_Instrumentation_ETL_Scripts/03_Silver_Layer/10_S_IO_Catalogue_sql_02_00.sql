
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
and Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1



