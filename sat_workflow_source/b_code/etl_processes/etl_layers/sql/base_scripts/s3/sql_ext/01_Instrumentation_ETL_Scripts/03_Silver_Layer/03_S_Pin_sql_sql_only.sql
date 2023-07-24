
-- MAGIC %sql

 Create Or Replace Temp View VW_EL_Pin_Prep_Query
 As
 Select Distinct
  A.database_name
 ,A.object_identifier
 ,A.Dynamic_Class
 ,A.LC_function_pin_rel_dyn_class as Function_Dynamic_class
 ,A.LC_function_pin_rel_href as Function_Object_Identifier
 ,A.LC_function_el_pin_group_view_overview as group_overview

 ,A.LC_function_pin_overview_pin_number as overview_pin_number
 ,A.LC_function_pin_internal_pin_number as internal_pin_number
 ,case when A.LC_function_pin_designation not like '%g/c%' then A.LC_function_pin_designation END as pin_designation

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

 Inner join sigraph_Silver.S_ItemFunction F 
 On A.database_name=F.database_name 
 and A.LC_function_pin_rel_dyn_class=F.dynamic_class
 and A.LC_function_pin_rel_href=F.Object_identifier

 Left outer join (
 Select *
 From sigraph.Component_Function
 Qualify Row_Number() Over(Partition by Database_name,Dynamic_Class,Object_Identifier order by Object_Identifier)=1
 ) as B 
 On A.database_name=B.database_name 
 and A.LC_function_pin_rel_dyn_class=B.dynamic_class
 and A.LC_function_pin_rel_href=B.Object_identifier

 Left outer join sigraph.Pin_group PG 
 On A.database_name=PG.database_name 
 and A.LC_Pin_group_Function_el_pin_Rel_href=PG.object_identifier

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
      ) =1;

Create Or Replace Temp View VW_EL_Pin
As
Select Distinct
 A.Database_Name
,A.Object_Identifier
,A.Dynamic_Class
,A.Function_Dynamic_Class
,A.Function_Object_Identifier
,A.Group_Overview
,A.Overview_Pin_Number
,A.Internal_Pin_Number
,Pin_Group
,Potential
,Pin_Designation
,Component_Function_Designation
,Generate_Pin
,Concat(Pin_Group,potential
      ,Case -- For terminal strip where we have different pin designation for same pin like pin=1 has ABCD, then apply below logic
            when IsTerminalStrip=1 and Pin_Side_Count>=4 and Pin_Component<>''
            Then Replace(TerminalSides,',',Pin_Component)
            -- For terminal strip where we have only 2 pin designation A,B for same pin like pin=1 has A&B, then apply below logic
            when IsTerminalStrip=1 and Pin_Side_Count<4 and Pin_Component<>''
            Then Pin_Component
            -- This condition is added for those devices like Gateway and others, where we dont have terminals defined.
            when IsPLC=0 and DeviceWithNoPin=1
            Then Generate_Pin     
			 else Coalesce(Pin_Designation,Pin_Component) 
        END)
         as Terminal_Marking 
from VW_EL_Pin_Prep_Query A      
Where 
-- Ignore Pin if They are PLC Module and has no pins. As these terminals marking will be loaded separately per Channel number
Case When IsPLC=1 and DeviceWithNoPin=1 then 0 else 1 end=1;

Create Or Replace Temp View VW_Pin_Union
As
Select 
Database_Name	
,Object_Identifier	
,Dynamic_Class
,Function_Dynamic_class	
,Function_Object_Identifier	
,Group_Overview	
,Overview_Pin_Number	
,Internal_Pin_Number	
,Pin_Group	
,Potential	
,Pin_Designation	
,Component_Function_Designation	
,Generate_Pin
,Case when Terminal_Marking ='.' Then 'NA100' 
      when Terminal_Marking ='..' Then 'NA101' 
      when Terminal_Marking ='...' Then 'NA102' 
      when Terminal_Marking =':' Then 'NA103'
      ELSE Terminal_Marking END as Terminal_Marking
,'EL_PIN' as Pin_Type
From VW_EL_Pin;


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
Where PN.RNT=1;

-- MAGIC %sql
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

 Left outer join (
 Select *
 From sigraph.Component_Function
 Qualify Row_Number() Over(Partition by Database_name,Dynamic_Class,Object_Identifier order by Object_Identifier)=1
 ) as B  On A.database_name=B.database_name and A.LC_function_pin_rel_dyn_class=B.dynamic_class
 and A.LC_function_pin_rel_href=B.Object_identifier

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
      )=1;

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