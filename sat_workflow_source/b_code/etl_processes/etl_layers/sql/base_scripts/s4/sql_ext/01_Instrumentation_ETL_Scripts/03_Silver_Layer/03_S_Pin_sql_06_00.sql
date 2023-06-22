
Create Or Replace Temp View VW_Pin_Final
As

Select 
 A.Database_Name	
,A.Object_Identifier	
,A.Dynamic_Class
,A.Function_Dynamic_class	
,A.Function_Object_Identifier	
,A.Group_Overview	
,A.Overview_Pin_Number	
,A.Internal_Pin_Number	
,A.Pin_Group	
,A.Potential	
,A.Pin_Designation	
,A.Component_Function_Designation
,A.Pin_Type
,A.Terminal_Marking
,A.Terminal_Side
From VW_Pin A

UNION

Select 
 A.Database_Name	
,A.Object_Identifier	
,A.Dynamic_Class
,A.Function_Dynamic_class	
,A.Function_Object_Identifier	
,A.Group_Overview	
,A.Overview_Pin_Number	
,A.Internal_Pin_Number	
,A.Pin_Group	
,A.Potential	
,A.Pin_Designation	
,A.Component_Function_Designation
,'BR_PIN' as Pin_Type
,B.Terminal_Marking
,Case When LCon.Object_Identifier is not null Then 'Left'
      When RCon.Object_Identifier is not null Then 'Right'
      Else B.Terminal_Side END as Terminal_Side
From VW_BR_Pin A
INNER join (
SELECT
*
From VW_Pin B
QUALIFY Row_Number() Over(PARTITION BY Database_name,Function_Dynamic_Class,Function_Object_identifier 
order by Terminal_Marking,object_identifier)=1
) as B  ON A.database_name=B.database_name and A.Function_Dynamic_Class=B.Function_Dynamic_Class
and A.Function_Object_Identifier=B.Function_Object_identifier
-- Left
left outer join (
Select 
Con.object_identifier
,Con.database_name
,Coalesce(Con.To_connection_pin_href,WF.to_LC_Wire_function_Function_pin_href) as To_Pin_Object_Identifier
,Coalesce(Con.To_connection_pin_dyn_class,WF.To_LC_Wire_function_Function_pin_dyn_class) as To_Pin_Dynamic_Class
From Sigraph.Connection Con
Left outer join sigraph.Wire_function WF 
ON WF.database_name=Con.database_name 
and WF.LC_wire_connection_rel_href = Con.object_identifier
) LCon On LCon.database_name=A.database_name and LCon.To_Pin_Dynamic_Class=A.Dynamic_Class
and LCon.To_Pin_Object_Identifier=A.Object_Identifier
-- Right
left outer join (
Select 
Con.object_identifier
,Con.database_name
,Coalesce(Con.From_connection_pin_href,WF.from_LC_Wire_function_Function_pin_href) as From_Pin_Object_Identifier
,Coalesce(Con.from_connection_pin_dyn_class,WF.from_LC_Wire_function_Function_pin_dyn_class) as From_Pin_Dynamic_Class
From Sigraph.Connection Con
Left outer join sigraph.Wire_function WF 
ON WF.database_name=Con.database_name 
and WF.LC_wire_connection_rel_href = Con.object_identifier
) RCon On RCon.database_name=A.database_name and RCon.From_Pin_Dynamic_Class=A.Dynamic_Class
and RCon.From_Pin_Object_Identifier=A.Object_Identifier


