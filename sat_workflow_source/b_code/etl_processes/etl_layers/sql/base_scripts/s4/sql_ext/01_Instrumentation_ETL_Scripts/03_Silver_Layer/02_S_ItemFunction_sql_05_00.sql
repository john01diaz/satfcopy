
-- MAGIC %sql
 CREATE OR REPLACE TEMP VIEW VW_ItemFunction_Prep_Query2
 As
 Select Distinct
 A.database_name
 ,A.dynamic_class
 ,A.object_identifier
 ,A.Function_Occ_Object_identifier
 ,A.item_dynamic_class 
 ,A.item_object_identifier 
 ,A.Location_Dynamic_class
 ,A.Location_Object_Identifier
 ,A.Facility_Dynamic_class
 ,A.Facility_Object_Identifier
 ,A.Rack_Dynamic_class
 ,A.Rack_Object_Identifier
 ,A.ModelNo
 ,A.Description
 ,A.IOType
 ,A.ChannelNumber
 ,A.product_designation
 ,A.Show_Key
 ,A.Item_Slot
 ,A.product_key
 ,Coalesce(B.Tag_Number,C.Tag_Number) as Tag_Number
 ,Coalesce(B.Loop_Number,C.Loop_Number) as Loop_Number
 ,Coalesce(B.Document_Number,C.Document_Number) as Document_Number
 ,Coalesce(B.Class,C.Class,'Inst(Shared)') as Class
 ,A.Type
 -- If the terminal strip is in field, then add the terminal strip tag as a cabinet no or location.
 -- As in Aveva, terminal strip and device cannot be loaded without cabinet no.
 ,Case When Coalesce(B.Tag_Number,C.Tag_Number) is null and A.Location_Designation is null --and Type='Terminal Strip'
       Then A.product_key Else A.Location_Designation END as Location_Designation
 ,Facility_Designation   
 ,Rack
 -- Max is added to have same entry for all the item object identifier's records.
 ,MAX(Coalesce(B.Manufacturer,C.Manufacturer)) Over(Partition by A.database_name,A.Item_Object_Identifier) as Manufacturer
 ,MAX(Coalesce(B.Device_Type,C.Device_Type)) Over(Partition by A.database_name,A.Item_Object_Identifier) as Device_Type
 ,A.Symbol_Name    
 ,Coalesce(B.loop_element_dynamic_class,C.loop_element_dynamic_class) as loop_element_dynamic_class
 ,Coalesce(B.loop_element_Object_Identifier,C.loop_element_Object_Identifier) as loop_element_Object_Identifier
 From VW_ItemFunction_Prep_Query1  A
 -- Ignore Template loop Function items.
 LEFT ANTI JOIN VW_Loop_Tag_DocumentNumber LF ON  A.database_name=LF.loop_database_name
 and A.object_identifier = LF.LC_Item_function_CS_Loop_element_href
 and A.dynamic_class = LF.LC_Item_function_CS_Loop_element_dyn_class
 and LF.Template_Loop='TRUE'

 left outer join VW_Loop_Tag_DocumentNumber B 
 ON  A.database_name=B.loop_database_name
 and A.object_identifier = B.LC_Item_function_CS_Loop_element_href
 and A.dynamic_class = B.LC_Item_function_CS_Loop_element_dyn_class
 -- The instruments where dynamic class and object identifier mapping is not created but we still have that instrument in Sigraph. As per Uli (Sigraph Developer), below logic has been implemented    
 left outer join VW_Loop_Tag_DocumentNumber C 
 ON C.loop_database_name =A.database_name
 and regexp_extract(C.Tag_Number,('^[0-9]+'),0)=A.facility_designation
 and regexp_extract(C.Tag_Number,'([A-Z]+)([0-9]+)',0)=A.product_key
 and C.LC_Item_function_CS_Loop_element_href is null

