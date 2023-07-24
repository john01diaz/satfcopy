
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
    from Sigraph_silver_S_Itemfunction A

    Inner join Sigraph_silver_S_Item_Function_Model B
    On A.database_name=B.database_name
    and A.Item_Dynamic_Class=B.Item_Dynamic_Class
    and A.Item_Object_identifier=B.Item_Object_identifier
    and A.Dynamic_Class=B.Dynamic_Class
    and A.Object_Identifier=B.Object_Identifier

    Where Type='IO Module'
    and ChannelNumber is not null
    and ChannelNumber<>'0'
    and ChannelNumber<>''