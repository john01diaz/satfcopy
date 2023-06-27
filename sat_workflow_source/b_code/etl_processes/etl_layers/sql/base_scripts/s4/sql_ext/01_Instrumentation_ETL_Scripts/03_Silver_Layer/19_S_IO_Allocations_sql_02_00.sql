
create or replace temp view IO_Allocations_Instrument_Mapping
as
Select Distinct
 SR.database_name
,SR.Dynamic_Class
,SR.object_identifier
,SR.Item_Object_identifier
,SR.Item_Dynamic_Class 
,Coalesce(SR.Loop_Number,TL.Loop_Number) as Loop_Number
,TL.Tag_Number as Tag_Number
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
-- Get the instrument tag number
Inner join (
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
and TL.Loop_Number=SR.Loop_Number
and TL.Function_Dynamic_Class = SR.To_Dynamic_Class
and TL.Function_Object_identifier=SR.To_Object_Identifier

-- If we have one instrument per loop, then map the IO card and its channel to the single instrument using below query.
Left outer join (
Select 
database_name
,Loop_Number
,Tag_Number
,Count(Tag_Number) Over(Partition by database_name,Loop_Number) as InstrumentCount
From (
Select Distinct database_name,Loop_Number,Tag_Number from  Sigraph_silver.S_Connection Where From_Location is null  
UNION
Select Distinct database_name,Loop_Number,Tag_Number from  Sigraph_silver.S_Connection Where To_Location is null  
) as A
) as Ins 
ON Ins.InstrumentCount=1 
and Ins.database_name=SR.database_name 
and Ins.Loop_Number=SR.Loop_Number


