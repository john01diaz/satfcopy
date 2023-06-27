
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
   and B.ModelNo Not like '%10101/2/1%'

