
Create Or Replace Temp View VW_Instrument_Identification
As
-- Based on Function Class--> Type Column
With CTE 
 (loop_element_database_name
,loop_element_dynamic_class
,loop_element_Object_Identifier
,Type
)
As (
Select 
A.loop_element_database_name
,A.loop_element_dynamic_class
,A.loop_element_Object_Identifier
,1 as Type
from Sigraph.Loop_Elements A 
Inner join Sigraph_Silver.S_ItemFunction B 
On  A.Loop_Element_Database_Name     = B.database_name
and A.loop_element_dynamic_class     = B.loop_element_dynamic_class
and A.loop_element_Object_Identifier = B.loop_element_Object_Identifier
Where CS_Loop_CS_Loop_element_dyn_class = 'CS_Loop_spez' 
and B.Type='Field Device'
and A.LC_Item_function_CS_Loop_element_dyn_class is not null 
)

Select * from CTE

UNION
-- If we dont have Function Class for the given loop element then validate using below condition.
Select 
A.loop_element_database_name
,A.loop_element_dynamic_class
,A.loop_element_Object_Identifier
,2 as Type
from Sigraph.Loop_Elements A 

LEFT ANTI JOIN CTE B ON A.loop_element_database_name=B.loop_element_database_name
and A.loop_element_dynamic_class=B.loop_element_dynamic_class 
and A.loop_element_Object_Identifier=B.loop_element_Object_Identifier

left outer join sigraph_Reference.DeviceType as DTC ON  UPPER(TRIM(DTC.SigraphDeviceType))=UPPER(TRIM(A.CS_device_type)) 

Where CS_Loop_CS_Loop_element_dyn_class='CS_Loop_spez' 
and A.LC_Item_function_CS_Loop_element_dyn_class is null
and Case When CS_location_full_designation is null Or UPPER(CS_device_type) like '%INDUCTIVE%SENSOR%' Then 1 else 0 End=1
-- Ignore the Soft tags.
and loop_element_dynamic_class not in ('CS_Loop_element_hw_bi'
,'CS_Loop_element_hw_bo'
,'CS_Loop_element_hw_ai'
,'CS_Loop_element_hw_ao')
and TRIM(
       Coalesce(Replace(Replace(Replace(Replace(RevisedDeviceType,'ANALOG INPUT','AI'),'ANALOG OUTPUT','AO')
                 ,'DIGITAL INPUT','DI'),'DIGITAL OUTPUT','DO')
        ,A.CS_device_type,'')
        ) 
Not in 
 ('C300 DO','DCS AI','FTA DO','IOTA AI','PLC DO','PLS AO','PLS DI','PLS DO','PLS OUTBOUND','SM AI','SM DI','SM DO','SPS AI','SPS DI')

