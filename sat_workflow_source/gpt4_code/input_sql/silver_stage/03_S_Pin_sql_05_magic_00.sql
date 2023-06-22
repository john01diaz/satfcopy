

-- Get the jumper pin details and assign the same terminal markings that of electric pins.
Create Or Replace Temp View VW_BR_Pin
As
Select
A.Database_Name	
,A.Object_Identifier	
,A.Dynamic_Class
,A.LC_function_pin_rel_dyn_class as Function_Dynamic_class
,A.LC_function_pin_rel_href as Function_Object_Identifier
,Null as group_overview
,A.LC_function_pin_overview_pin_number as overview_pin_number
,A.LC_function_pin_internal_pin_number as internal_pin_number
,'' as Pin_group
,'' as potential	
,case when A.LC_function_pin_designation not like '%g/c%' then A.LC_function_pin_designation END as pin_designation	
,Coalesce(LC_component_function_designation,'') as component_function_designation
From sigraph.BR_Pin A
-- MAGIC
Left outer join (
Select *
From sigraph.Component_Function
Qualify Row_Number() Over(Partition by Database_name,Dynamic_Class,Object_Identifier order by Object_Identifier)=1
) as B  On A.database_name=B.database_name and A.LC_function_pin_rel_dyn_class=B.dynamic_class
and A.LC_function_pin_rel_href=B.Object_identifier
-- MAGIC
-- If the pin is present in overview and other function class, then consider the function class pins
Qualify Row_Number() Over(Partition by 
                  A.Database_name,A.Object_Identifier 
                  order by 
                  Case When 
                   A.LC_function_pin_rel_dyn_class in 
                   (
                   'LC_PLC_Function','LC_General_function','LC_Integrated_function','LC_Component_function'
                   ) 
                 Then 1 
                 When A.LC_function_pin_rel_dyn_class='LC_PLC_Overview' 
                 Then 2 
                 Else 3 END
     )=1

