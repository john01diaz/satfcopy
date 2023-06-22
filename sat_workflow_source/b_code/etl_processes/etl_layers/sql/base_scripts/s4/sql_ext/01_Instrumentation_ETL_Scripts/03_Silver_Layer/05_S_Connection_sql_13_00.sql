
Create Or Replace TEMP VIEW VW_Connection_Final_Extract
AS
Select Distinct
Con.object_identifier
,con.database_name
,Con.From_Pin_Object_Identifier as from_connection_pin_href
,Con.To_Pin_Object_Identifier as To_connection_pin_href
,Con.FROM_dynamic_Class
,Con.FROM_object_identifier
,Con.FROM_Item_dynamic_Class
,Con.FROM_Item_object_identifier
,Con.To_dynamic_Class
,Con.To_object_identifier
,Con.To_Item_dynamic_Class
,Con.To_Item_object_identifier
,Con.Cable_Object_Identifier
,Con.Wire_Object_Identifier
,Con.From_Location
,Con.From_Facility
,Con.From_Item
,Con.From_Terminal_Marking
-- Define the fabricated cable, by takeing the database name and the connectionid . So that there will be unique cable id per connection. Also the cross section will be 1 and core markings will be 1.
,Coalesce(Con.Cable,Concat(Replace(Con.database_name,'_2016R3',''),'_',Con.Object_Identifier)) as Cable
,Coalesce(Con.Wire_Cross_Section,1) as Wire_Cross_Section
,Coalesce(Con.Wire_Markings,1) as Wire_Markings
-- End of fabricated cable changes
,Con.To_Location
,Con.To_Facility 
,Con.To_Item
,Con.To_Terminal_Marking
,Coalesce(Con.Loop_Number,CFL.Loop_Number,CFLT.Loop_Number) as Loop_Number
,Con.Tag_Number
,Coalesce(Con.Document_Number,CFL.Document_Number,CFLT.Document_Number) as Document_Number
--A normal electrical connection between Terminals (LC_Component_function) of the same Terminal Strip, is always a Jumper connection.
,Case  
      when Con.Wire_Markings is null and 
      Con.From_Item_Dynamic_class=Con.To_Item_Dynamic_class 
      and Con.From_Item_Object_Identifier=Con.To_Item_Object_Identifier
      and Con.From_Item_Dynamic_class='LC_Terminal_strip' 
      and Con.From_Dynamic_class=Con.To_Dynamic_Class and Con.From_Object_Identifier<>Con.To_Object_Identifier
      Then 'Link'
      when Con.Wire_Markings is not null and 
      Con.From_Item_Dynamic_class=Con.To_Item_Dynamic_class 
      and Con.From_Item_Object_Identifier=Con.To_Item_Object_Identifier
      and Con.From_Item_Dynamic_class='LC_Terminal_strip' 
      and Con.From_Dynamic_class=Con.To_Dynamic_Class and Con.From_Object_Identifier<>Con.To_Object_Identifier
      Then 'Loop'
      Else 'Wire'
      END as Connection_Type
From VW_Connection_Prep_Query Con
--Map loop ID's to Component Function Class to get end to end connection mapped to loop
Left outer join VW_Component_Function_Loop CFL 
ON CFL.RNT=1 and CFL.database_name=Con.database_name 
and CFL.dynamic_Class=Con.From_dynamic_Class 
and CFL.object_identifier=Con.From_object_identifier
--Map loop ID's to Component Function Class to get end to end connection mapped to loop
Left outer join VW_Component_Function_Loop CFLT 
ON CFLT.RNT=1 
and CFLT.database_name=Con.database_name 
and CFLT.dynamic_Class=Con.To_dynamic_Class and CFLT.object_identifier=Con.To_object_identifier



