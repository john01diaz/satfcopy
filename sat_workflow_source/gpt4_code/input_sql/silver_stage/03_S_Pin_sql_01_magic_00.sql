

-- MAGIC
Create Or Replace Temp View VW_EL_Pin_Prep_Query
As
Select Distinct
 A.database_name
,A.object_identifier
,A.Dynamic_Class
,A.LC_function_pin_rel_dyn_class as Function_Dynamic_class
,A.LC_function_pin_rel_href as Function_Object_Identifier
,A.LC_function_el_pin_group_view_overview as group_overview
-- MAGIC
,A.LC_function_pin_overview_pin_number as overview_pin_number
,A.LC_function_pin_internal_pin_number as internal_pin_number
,case when A.LC_function_pin_designation not like '%g/c%' then A.LC_function_pin_designation END as pin_designation
-- MAGIC
-- Below will be used to define the terminal marking
,Coalesce(PG.LC_Pin_group_designation,'') as Pin_group
,Coalesce(A.LC_function_pin_potential,'') as potential
,Coalesce(LC_component_function_designation,'') as component_function_designation
,Case when A.LC_function_pin_rel_dyn_class='LC_Component_function' 
      and Coalesce(A.LC_function_pin_designation,'') in ('A','B','C','D','E','F','G','H','I','J') 
      Then 1 Else 0 END as IsTerminalStrip 
,Coalesce(B.LC_component_function_designation,A.LC_function_pin_overview_pin_number,'') as Pin_Component
,to_get_sort(collect_set(Coalesce(A.LC_function_pin_designation,'')) 
                    over(partition by A.database_name,A.LC_function_pin_rel_href
                    -- Below script will group the Sides if we have more than 2 terminals for same component object id.
                    ,Case When Coalesce(A.LC_function_pin_designation,'') in ('A','B') Then 1
                          When Coalesce(A.LC_function_pin_designation,'') in ('C','D') Then 2
                          When Coalesce(A.LC_function_pin_designation,'') in ('E','F') Then 3
                          When Coalesce(A.LC_function_pin_designation,'') in ('G','H') Then 4
                          ELSE 5 END
                    )) as TerminalSides
,Count(1) 
    Over(Partition by A.database_name,A.LC_function_pin_rel_dyn_class
    ,A.LC_function_pin_rel_href,Coalesce(LC_component_function_designation,''))   as Pin_Side_Count
,Case When      Coalesce(A.LC_function_pin_designation,0)        =0 
            and Coalesce(B.LC_component_function_designation,0)  =0 
            and Coalesce(A.LC_function_pin_overview_pin_number,0)=0
            and MAX(
      Coalesce(A.LC_function_pin_designation,B.LC_component_function_designation,A.LC_function_pin_overview_pin_number,0)
                  ) Over(PARTITION BY F.database_name,F.item_object_identifier)=0
       Then 1 else 0 END as DeviceWithNoPin
,Dense_Rank() Over(Partition by F.database_name,F.Item_object_identifier order by A.object_identifier) as Generate_Pin
-- Ignore Pin if They are PLC Module and has no pins. As these terminals marking will be loaded separately per Channel number
,Case When F.Item_Dynamic_Class='LC_PLC_Module'  Then 1 Else 0 End as IsPLC
--
,'EL_PIN' as Type
From sigraph.LC_Function_el_pin A
-- MAGIC
Inner join sigraph_Silver.S_ItemFunction F 
On A.database_name=F.database_name 
and A.LC_function_pin_rel_dyn_class=F.dynamic_class
and A.LC_function_pin_rel_href=F.Object_identifier
-- MAGIC
Left outer join (
Select *
From sigraph.Component_Function
Qualify Row_Number() Over(Partition by Database_name,Dynamic_Class,Object_Identifier order by Object_Identifier)=1
) as B 
On A.database_name=B.database_name 
and A.LC_function_pin_rel_dyn_class=B.dynamic_class
and A.LC_function_pin_rel_href=B.Object_identifier
-- MAGIC
Left outer join sigraph.Pin_group PG 
On A.database_name=PG.database_name 
and A.LC_Pin_group_Function_el_pin_Rel_href=PG.object_identifier
-- MAGIC
Left Outer join Sigraph.El_pin_occ OCC
On A.database_name=Occ.database_name
and A.dynamic_Class=Occ.Function_Pin_Dynamic_Class
and A.object_identifier=Occ.Function_Pin_Object_Identifier
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
     ) =1

