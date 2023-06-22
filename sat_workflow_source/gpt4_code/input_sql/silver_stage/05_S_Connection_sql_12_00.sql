
-- The terminal strip is not mapped to the loop and loop elements. Below query will help in mapping the components/terminal strips to loops and loop elements.
Create Or Replace TEMP View VW_Component_Function_Loop
As
Select 
A.database_name
,A.dynamic_Class
,A.object_identifier
,A.Tag_Number as Tag_Number 
,A.Loop_Number as Loop_Number
,A.Document_Number as Document_Number
,Row_Number() Over(Partition by A.database_name,object_identifier order by Loop_Number) as RNT
From (
Select Distinct
Con.database_name
,Con.To_dynamic_Class as dynamic_Class
,Con.To_object_identifier as object_identifier
,LT.Tag_Number as Tag_Number 
,LT.Loop_Number as Loop_Number
,LT.Document_Number as Document_Number
From VW_Connection_Prep_Query Con
Inner join VW_Loop_Tag_DocumentNumber LT 
ON LT.loop_database_name=Con.database_name 
and LT.LC_Item_function_CS_Loop_element_dyn_class=Con.From_dynamic_Class
and LT.LC_Item_function_CS_Loop_element_href=Con.From_object_identifier
Where To_dynamic_Class='LC_Component_function'
UNION
Select Distinct
Con.database_name
,Con.From_dynamic_Class
,Con.From_object_identifier
,LT.Tag_Number as Tag_Number 
,LT.Loop_Number as Loop_Number
,LT.Document_Number as Document_Number
From VW_Connection_Prep_Query Con
Inner join VW_Loop_Tag_DocumentNumber LT 
ON LT.loop_database_name=Con.database_name 
and LT.LC_Item_function_CS_Loop_element_dyn_class=Con.To_dynamic_Class
and LT.LC_Item_function_CS_Loop_element_href=Con.To_object_identifier
Where From_dynamic_Class='LC_Component_function'
) as A

