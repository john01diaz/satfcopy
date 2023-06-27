
create or replace temp view IO_Allocations_Device_Mapping
as
Select Distinct
 SR.database_name
,SR.Dynamic_Class
,SR.object_identifier
,SR.Item_Object_identifier
,SR.Item_Dynamic_Class 
,Coalesce(SR.Loop_Number,DV.Loop_Number) as Loop_Number
,DV.Product_Key as Tag_Number
,SR.Parent_Equipment_No
,SR.IOType
,SR.Equipment_No
,SR.ChannelNumber
,SR.Class
,SR.Document_Number
,SR.Junction_Box   
,SR.To_dynamic_Class  
,SR.To_object_Identifier
From IO_Allocations_Prep_Query SR
-- Ignore the IO Channels where instrument mapping is already done
LEFT ANTI JOIN IO_Allocations_Instrument_Mapping IM ON IM.database_name=SR.database_name 
and IM.object_identifier=SR.Object_identifier

-- Ignore the instrument tag number
LEFT ANTI join (
Select
L.loop_database_name as database_name
,L.CS_loop_ID as Loop_Number
,TL.CS_loop_element_id as Tag_Number
,TL.LC_Item_function_CS_Loop_element_dyn_class as Function_Dynamic_Class
,TL.LC_Item_function_CS_Loop_element_href as Function_Object_identifier
From sigraph.loop L 
Inner join sigraph.loop_elements TL ON TL.loop_element_database_name=L.Loop_database_name
and L.loop_dynamic_class=TL.CS_Loop_CS_Loop_element_dyn_class
and L.loop_object_identifier=TL.CS_Loop_CS_Loop_element_href
-- Restrict the data only for field device
INNER join sigraph_silver.S_Field_Device_Catalogue  FD 
on  FD.database_name==TL.loop_element_database_name
and FD.dynamic_class=TL.loop_element_dynamic_class
and FD.Object_Identifier=TL.loop_element_Object_Identifier
) as TL ON TL.database_name=SR.database_name 
and TL.Function_Dynamic_Class = SR.To_Dynamic_Class
and TL.Function_Object_identifier=SR.To_Object_Identifier


-- Get the Device tag number
Inner join (
select
A.database_name
,A.Dynamic_class
,A.object_Identifier
,A.Product_Key
,Coalesce(A.Loop_Number,B.Loop_Number) as Loop_Number
From Sigraph_silver.S_Itemfunction A
left outer join Sigraph_Silver.S_IO_Allocation_Loop_Mapping B ON A.object_identifier=B.From_object_identifier
UNION
select
A.database_name
,A.Dynamic_class
,A.object_Identifier
,A.Product_Key
,Coalesce(A.Loop_Number,B.Loop_Number) as Loop_Number
From Sigraph_silver.S_Itemfunction A
left outer join Sigraph_Silver.S_IO_Allocation_Loop_Mapping B ON A.object_identifier=B.To_object_identifier
) as DV
ON DV.database_name=SR.Database_name 
and DV.Loop_Number=SR.Loop_Number
and DV.Dynamic_Class=SR.To_Dynamic_Class
and DV.Object_identifier=SR.To_Object_Identifier 



