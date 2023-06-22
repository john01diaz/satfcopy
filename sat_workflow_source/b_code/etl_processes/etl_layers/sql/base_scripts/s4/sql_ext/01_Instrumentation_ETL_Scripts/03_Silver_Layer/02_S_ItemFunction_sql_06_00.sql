
CREATE OR REPLACE TEMP VIEW VW_ItemFunction_Prep_Query
As
Select Distinct
A.database_name
,A.dynamic_class
,A.object_identifier
,Function_Occ_Object_identifier
,A.item_dynamic_class
,A.item_object_identifier
,Location_Dynamic_class
,Location_Object_Identifier
,Facility_Dynamic_class
,Facility_Object_Identifier
,Rack_Dynamic_class
,Rack_Object_Identifier
,A.ModelNo
,Description
,IOType
-- The below dense rank condition is added, as for same PLC Module we have Analog and Digital Channels. So it would be duplicate if we try to load only channel numbers + and -, so wherever we have duplicate, we prefixed with IO Type.
,Case When ChannelNumber<>'' and IOType<>''
      and Dense_Rank() Over(partition by A.Database_name,A.item_dynamic_class,A.Dynamic_Class,A.item_object_identifier 
                            order by IOType)>1
      Then Concat(IOType,'_',ChannelNumber)
      Else ChannelNumber END as ChannelNumber
,product_designation
-- If we have same product twice in same cabinet, add the sequence like .1, .2 at the end of the product key
-- Confirmed by Detlef
,Case 
     when location_designation is not null and 
     Dense_Rank(product_key) Over(Partition by location_designation,product_key 
     order by A.database_name,A.Item_Object_identifier)>1 
     Then Concat(product_key,'.',
        Dense_Rank(product_key) Over(Partition by location_designation,product_key 
        order by A.database_name,A.Item_Object_identifier)
                ) 
     Else product_key 
     END as product_key 
,product_key as product_key_Original
,Show_Key
,Item_Slot
,Tag_Number
,Loop_Number
,Document_Number
,Class
,Case When Tag_Number is not null and Location_Designation is null 
      Then 'Field Device' 
      When Tag_Number is not null and Upper(Coalesce(A.device_type,B.device_type,'RHLDD')) like '%INDUCTIVE%SENSOR%'
      Then 'Field Device'
      Else Type
      END as Type
,Location_Designation
,Facility_Designation
,Rack
,Coalesce(A.manufacturer,B.manufacturer,'RHLDD') as manufacturer
,Coalesce(A.device_type,B.device_type,'RHLDD') as device_type
,Coalesce(A.Symbol_Name,B.Symbol_Name) as Symbol_Name
,loop_element_dynamic_class
,loop_element_Object_Identifier
From (
SELECT
A.*
,Count(1) Over(partition by database_name,Item_Dynamic_Class,Item_Object_Identifier)  as NoOfChannels
From  VW_ItemFunction_Prep_Query2 A
-- ignore the unused pins or redundant pins -- Check the email from Jakob on Jan 24 2023
LEFT ANTI JOIN (
Select Distinct
A.database_name,A.dynamic_Class,A.Object_identifier
From VW_ItemFunction_Prep_Query2 A
LEFT SEMI JOIN (
Select Distinct database_name,Item_dynamic_class,Item_Object_Identifier,Product_Key
From VW_ItemFunction_Prep_Query2
Where Type='Field Device'
) as B On A.database_name=B.database_name and A.Item_dynamic_class=B.Item_dynamic_class
and A.Item_Object_Identifier=B.Item_Object_Identifier and A.Product_Key=B.Product_Key
Where Type='Device'
) As RDP ON RDP.database_name=A.database_name and RDP.dynamic_Class=A.dynamic_Class 
and RDP.Object_identifier=A.Object_identifier
) As A

-- If device is nto having symbol name then check below condition and assign the symbol name.
LEFT OUTER JOIN (
Select Distinct
database_name
,dynamic_class
,item_dynamic_class
,Symbol_Name
,ModelNo
,manufacturer
,device_type
,Count(1) Over(partition by database_name,Item_Dynamic_Class,Item_Object_Identifier) as NoOfChannels
FROM VW_ItemFunction_Prep_Query2
where Symbol_Name is not null
) as B On A.database_name=B.database_name and A.dynamic_class=B.dynamic_class and A.item_dynamic_class=B.item_dynamic_class
and A.ModelNo=B.ModelNo and A.NoOfChannels=B.NoOfChannels
-- Get unique record per database_name and object_identifier
Qualify Row_Number() Over(Partition by A.database_name,A.Object_identifier order by A.Object_Identifier)=1

