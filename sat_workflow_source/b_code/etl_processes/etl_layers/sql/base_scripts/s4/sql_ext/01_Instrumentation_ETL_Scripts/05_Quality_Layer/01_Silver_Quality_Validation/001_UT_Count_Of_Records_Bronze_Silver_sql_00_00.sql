-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW UT_VW_Bronze_Silver
AS
Select Distinct
 A.database_name
,A.object_identifier
,'185' as UT_ID
,CASE WHEN B.Object_Identifier is not null  then 'Pass' else 'Fail' end as Test_Case
From (
Select A.database_name,A.dynamic_class,A.object_identifier From sigraph.General_Function A
UNION
Select A.database_name,A.dynamic_class,A.object_identifier From sigraph.PLC_Function A
UNION
Select A.database_name,A.dynamic_class,A.object_identifier From sigraph.Component_Function A
UNION
Select A.database_name,A.dynamic_class,A.object_identifier From sigraph.Integrated_function A
UNION
Select A.database_name,A.dynamic_class,A.object_identifier From sigraph.PLC_Overview A
) as A
-- Ignore Template loops
LEFT ANTI JOIN (
Select
L.Loop_database_name as database_name 
,LC_Item_function_CS_Loop_element_dyn_class as Function_Dynamic_Class
,LC_Item_function_CS_Loop_element_href as Function_Object_Identifier
From sigraph.Loop L 
inner join Sigraph.Layer Layer
on  Layer.database_name      == L.Loop_database_name
and Layer.object_identifier  == L.Layer_CS_Loop_href
and Layer.dynamic_class      == L.Layer_CS_Loop_dyn_class
Inner join Sigraph.Loop_Elements LE ON LE.Loop_Element_database_name=L.Loop_database_name
and LE.CS_Loop_CS_Loop_element_dyn_class=L.loop_dynamic_class
and LE.CS_Loop_CS_Loop_element_href =L.loop_object_identifier 
Where Layer.Template_Loop='TRUE'
) as LF  ON  A.database_name=LF.database_name
and A.dynamic_class = LF.Function_Dynamic_Class
and A.object_identifier = LF.Function_Object_Identifier

Left outer join sigraph_silver.S_Itemfunction B ON A.database_name=B.database_name and A.Object_identifier=B.object_identifier
Where A.database_name='R_2016R3'


UNION

Select Distinct
 A.database_name
,A.object_identifier
,'186' as UT_ID
,CASE WHEN B.Object_Identifier is not null  then 'Pass' else 'Fail' end as Test_Case
From (
Select A.database_name,dynamic_class,A.object_identifier From sigraph.General_Item A
UNION
Select A.database_name,dynamic_class,A.object_identifier From sigraph.PLC_Module A
UNION
Select A.database_name,dynamic_class,A.object_identifier From sigraph.Terminal_Strip A
UNION
Select A.database_name,dynamic_class,A.object_identifier From sigraph.Integrated_Item A
) as A
-- Get only those items who has function object identifiers.
INNER JOIN
(
Select A.database_name,A.dynamic_class,A.object_identifier,A.LC_item_functions_rel_href,A.LC_item_functions_rel_dyn_class From sigraph.General_Function A
UNION
Select A.database_name,A.dynamic_class,A.object_identifier,A.LC_item_functions_rel_href,A.LC_item_functions_rel_dyn_class From sigraph.PLC_Function A
UNION
Select A.database_name,A.dynamic_class,A.object_identifier,A.LC_item_functions_rel_href,A.LC_item_functions_rel_dyn_class From sigraph.Component_Function A
UNION
Select A.database_name,A.dynamic_class,A.object_identifier,A.LC_item_functions_rel_href,A.LC_item_functions_rel_dyn_class From sigraph.Integrated_function A
UNION
Select A.database_name,A.dynamic_class,A.object_identifier,A.LC_item_functions_rel_href,A.LC_item_functions_rel_dyn_class From sigraph.PLC_Overview A
) as F ON F.database_name=A.database_name 
and F.LC_item_functions_rel_dyn_class=A.dynamic_class
and F.LC_item_functions_rel_href=A.ObjecT_Identifier
-- Ignore Template loops
LEFT ANTI JOIN (
Select
L.Loop_database_name as database_name 
,LC_Item_function_CS_Loop_element_dyn_class as Function_Dynamic_Class
,LC_Item_function_CS_Loop_element_href as Function_Object_Identifier
From sigraph.Loop L 
inner join Sigraph.Layer Layer
on  Layer.database_name      == L.Loop_database_name
and Layer.object_identifier  == L.Layer_CS_Loop_href
and Layer.dynamic_class      == L.Layer_CS_Loop_dyn_class
Inner join Sigraph.Loop_Elements LE ON LE.Loop_Element_database_name=L.Loop_database_name
and LE.CS_Loop_CS_Loop_element_dyn_class=L.loop_dynamic_class
and LE.CS_Loop_CS_Loop_element_href =L.loop_object_identifier 
Where Layer.Template_Loop='TRUE'
) as LF  ON  F.database_name=LF.database_name
and F.dynamic_class = LF.Function_Dynamic_Class
and F.object_identifier = LF.Function_Object_Identifier


Left outer join sigraph_silver.S_Itemfunction B ON A.database_name=B.database_name 
and A.dynamic_class=B.Item_dynamic_class
and A.Object_identifier=B.Item_object_identifier
Where A.database_name='R_2016R3'

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'187' as UT_ID
,CASE WHEN B.Object_Identifier is not null  then 'Pass' else 'Fail' end as Test_Case
From sigraph_silver.S_Itemfunction A
Left outer join Sigraph_Silver.S_Pin P ON A.database_name=P.database_name 
and A.Object_identifier=P.Function_Object_identifier

Left outer join sigraph_reference.Symbol_pin_orientation SP ON 
SP.Symbol_name=Case When A.Item_Dynamic_Class<>'LC_PLC_Module' Then A.Symbol_Name END
and Coalesce(SP.Top,SP.Right,SP.Bottom,SP.Left) is not null
Left outer join sigraph_silver.S_Item_Function_Model B ON A.database_name=B.database_name 
and A.Object_identifier=B.object_identifier
Where A.database_name='R_2016R3' and Coalesce(P.Object_identifier,SP.Symbol_Name) is not null
and Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1

UNION

Select Distinct
 A.database_name
,A.object_identifier
,'188' as UT_ID
,CASE WHEN B.Object_Identifier is not null  then 'Pass' else 'Fail' end as Test_Case
From (
Select A.database_name,A.object_identifier ,A.dynamic_class
From sigraph.LC_Function_el_pin A
LEFT SEMI JOIN Sigraph_silver.S_Itemfunction F ON A.database_name=F.database_name
and A.LC_function_pin_rel_href=F.object_identifier
Left outer join (
Select *
From sigraph.Component_Function
Qualify Row_Number() Over(Partition by Database_name,Dynamic_Class,Object_Identifier order by Object_Identifier)=1
) as B 
On A.database_name=B.database_name 
and A.LC_function_pin_rel_dyn_class=B.dynamic_class
and A.LC_function_pin_rel_href=B.Object_identifier
Where Case When Coalesce(B.LC_component_function_designation,A.LC_function_pin_designation) is not null Then 1
     When Coalesce(A.LC_function_pin_overview_pin_number,-1)>0 Then 1
     Else 0 END=1

UNION

Select A.database_name,A.object_identifier,A.dynamic_class 
From sigraph.BR_Pin A
LEFT SEMI JOIN Sigraph_silver.S_Itemfunction F ON A.database_name=F.database_name
and A.LC_function_pin_rel_href=F.object_identifier
Left outer join (
Select *
From sigraph.Component_Function
Qualify Row_Number() Over(Partition by Database_name,Dynamic_Class,Object_Identifier order by Object_Identifier)=1
) as B  On A.database_name=B.database_name and A.LC_function_pin_rel_dyn_class=B.dynamic_class
and A.LC_function_pin_rel_href=B.Object_identifier
Where Case When Coalesce(B.LC_component_function_designation,A.LC_function_pin_designation) is not null Then 1
     When Coalesce(A.LC_function_pin_overview_pin_number,-1)>0 Then 1
     Else 0 END=1

) as A
Left outer join sigraph_silver.S_Pin B ON A.database_name=B.database_name 
and A.Object_identifier=B.object_identifier
Where A.database_name='R_2016R3'


UNION

Select Distinct
 A.database_name
,A.object_identifier
,'189' as UT_ID
,CASE WHEN B.Object_Identifier is not null  then 'Pass' else 'Fail' end as Test_Case
From sigraph.Connection as A
Left outer join sigraph_silver.s_connection B ON A.database_name=B.database_name 
and A.Object_identifier=B.object_identifier
Where A.database_name='R_2016R3'



