
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
and  Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1


