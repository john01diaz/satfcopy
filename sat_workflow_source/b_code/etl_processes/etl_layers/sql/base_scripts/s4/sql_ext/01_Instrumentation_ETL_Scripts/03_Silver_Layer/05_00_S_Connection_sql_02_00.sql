
Create Or Replace Temp View VW_Connection_Pin_Unions
As
Select Distinct
FCon.object_identifier
,FCon.database_name
,From_Pin_Object_Identifier
,From_Pin_Dynamic_Class
,To_Pin_Object_Identifier
,To_Pin_Dynamic_Class
from (
Select 
Con.object_identifier
,Con.database_name
,Coalesce(Con.From_connection_pin_href,WF.from_LC_Wire_function_Function_pin_href) as From_Pin_Object_Identifier
,Coalesce(Con.from_connection_pin_dyn_class,WF.from_LC_Wire_function_Function_pin_dyn_class) as From_Pin_Dynamic_Class
From Sigraph.Connection Con
Left outer join sigraph.Wire_function WF 
ON WF.database_name=Con.database_name 
and WF.LC_wire_connection_rel_href = Con.object_identifier
Where Coalesce(Con.From_connection_pin_href,WF.from_LC_Wire_function_Function_pin_href) is not null
) as FCon
Inner join (
Select 
Con.object_identifier
,Con.database_name
,Coalesce(Con.To_connection_pin_href,WF.to_LC_Wire_function_Function_pin_href) as To_Pin_Object_Identifier
,Coalesce(Con.To_connection_pin_dyn_class,WF.To_LC_Wire_function_Function_pin_dyn_class) as To_Pin_Dynamic_Class
From Sigraph.Connection Con
Left outer join sigraph.Wire_function WF 
ON WF.database_name=Con.database_name 
and WF.LC_wire_connection_rel_href = Con.object_identifier
Where Coalesce(Con.To_connection_pin_href,WF.to_LC_Wire_function_Function_pin_href) is not null
) as TCon On FCon.database_name=TCon.Database_name and FCon.Object_Identifier=TCon.Object_Identifier

