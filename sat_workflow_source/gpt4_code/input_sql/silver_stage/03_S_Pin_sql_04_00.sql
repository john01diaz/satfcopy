

Create Or Replace Temp View VW_Pin
As
Select Distinct
 PN.Database_Name	
,PN.Object_Identifier	
,PN.Dynamic_Class
,PN.Function_Dynamic_class	
,PN.Function_Object_Identifier	
,PN.Group_Overview	
,PN.Overview_Pin_Number	
,PN.Internal_Pin_Number	
,PN.Pin_Group	
,PN.Potential	
,PN.Pin_Designation	
,PN.Component_Function_Designation	
,PN.Pin_Type
,Case --When PN.Terminal_Marking='0' OR PN.Terminal_Marking='' Then Concat('NA_',Generate_Pin)
      When PN.Terminal_Marking<>'0' and Function_Dynamic_class='LC_Component_function' 
      and --Terminal_Marking in ('N','PE','L-','L+','','AUS','EIN') and
      Dense_Rank() Over(Partition by PN.Database_name,PN.Item_Object_Identifier,PN.Terminal_Marking 
                        order by PN.Function_Object_Identifier)>1 
      Then Concat(
           Dense_Rank() Over(Partition by PN.Database_name,PN.Item_Object_Identifier,PN.Terminal_Marking 
           order by PN.Function_Object_Identifier)
           ,'_'
           ,PN.Terminal_Marking)
      Else PN.Terminal_Marking
      END as Terminal_Marking
,PN.Terminal_Side
From (
Select 
PN.*
,B.Item_Object_Identifier
,Case 
      When Pin_Type='EL_PIN' and PN.Function_Dynamic_Class='LC_Component_function' 
      and PN.Pin_Designation  in ('A','C','E','G','I')       Then 'Left'
      When Pin_Type='EL_PIN' and PN.Function_Dynamic_Class='LC_Component_function' 
      and PN.Pin_Designation  in ('B','D','F','H','J')       Then 'Right'
      When Pin_Type='EL_PIN' and Coalesce(S.Top,S.Right) is not null Then 'Left'
      When Pin_Type='EL_PIN' and Coalesce(S.Bottom,S.Left) is not null Then 'Right'
      When LCon.Object_Identifier is not null Then 'Left'
      When RCon.Object_Identifier is not null Then 'Right'
      Else 'Right'
      END as Terminal_Side   
,Row_Number()Over(Partition by PN.database_name,PN.Object_identifier,PN.Pin_Type order by PN.Object_Identifier) as RNT     
From VW_Pin_Union PN
Inner join Sigraph_Silver.S_Itemfunction B On PN.database_name=B.database_name 
and PN.Function_Dynamic_Class=B.Dynamic_Class
and PN.Function_Object_Identifier=B.Object_Identifier
-- Pin Orientation using Symbol
left outer join sigraph_reference.Symbol_pin_orientation S ON S.Symbol_name=B.Symbol_Name 
and PN.Internal_Pin_Number = Coalesce(S.Top,S.Right,S.Bottom,S.Left)
-- Pin Orientation using Connection
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
) LCon On LCon.database_name=PN.database_name and LCon.To_Pin_Dynamic_Class=PN.Dynamic_Class
and LCon.To_Pin_Object_Identifier=PN.Object_Identifier
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
) RCon On RCon.database_name=PN.database_name and RCon.From_Pin_Dynamic_Class=PN.Dynamic_Class
and RCon.From_Pin_Object_Identifier=PN.Object_Identifier
) as PN 
Where PN.RNT=1 

