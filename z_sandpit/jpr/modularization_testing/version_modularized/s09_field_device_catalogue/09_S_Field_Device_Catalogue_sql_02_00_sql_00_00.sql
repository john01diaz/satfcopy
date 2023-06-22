CREATE OR REPLACE TEMP VIEW VW_FieldDevice_PrepQuery
As
Select
  A.database_name,
  A.dynamic_class,
  A.object_identifier,
  A.Catalogue_Name,
  A.Left_pin_details,
  A.Left_Marking,
  A.Right_pin_details,
  A.Right_Marking,
  A.Tag_Number,
  A.Loop_Number,
  A.Document_number,
  A.Class,
  Row_Number() Over(
    Partition by A.Catalogue_Name
    order by A.Tag_Number
  ) as Catalogue_RNT
From
  (
    Select
        Distinct LE.Loop_Element_Database_name as database_name,
      LE.loop_element_dynamic_class as dynamic_class,
      LE.loop_element_Object_Identifier as object_identifier,
      Coalesce(VM.ModelNo,'') AS Catalogue_Name,
      Coalesce(VM.Left,0) as Left_pin_details,
      Coalesce(VM.Left_Marking, '') as Left_Marking,
      Coalesce(VM.Right,0) as Right_pin_details,
      Coalesce(VM.Right_Marking, '') as Right_Marking,
      LE.CS_Loop_Element_ID as Tag_Number,
      L.CS_Loop_ID as Loop_Number,
      L.DM_Document_Number as Document_number,
      Case When L.loop_dynamic_class='CS_Loop_spez' Then 'Instrumentation' Else 'Electrical' END as Class
     From sigraph.Loop L 
     
     Inner join Sigraph.Loop_Elements LE ON LE.Loop_Element_database_name=L.Loop_database_name
      and LE.CS_Loop_CS_Loop_element_dyn_class=L.loop_dynamic_class
      and LE.CS_Loop_CS_Loop_element_href =L.loop_object_identifier 
      
     Inner join sigraph.Layer 
      on  layer.database_name      == L.Loop_database_name
      and layer.object_identifier  == L.Layer_CS_Loop_href
      and layer.dynamic_class      == L.Layer_CS_Loop_dyn_class
      and Layer.Template_loop == "FALSE"
      -- Carve out Instrument from the loop element
      LEFT SEMI JOIN VW_Instrument_Identification INS ON INS.Loop_Element_Database_name=LE.Loop_Element_Database_name
      and INS.loop_element_dynamic_class=LE.loop_element_dynamic_class
      and INS.loop_element_Object_Identifier=LE.loop_element_Object_Identifier     
     
     Left outer join Sigraph_Silver.S_ItemFunction Item On Item.Database_name=LE.Loop_Element_Database_name
     and Item.loop_element_dynamic_class=LE.loop_element_dynamic_class
     and Item.loop_element_Object_Identifier=LE.loop_element_Object_Identifier
     
     Left Outer join sigraph_silver.S_Item_Function_Model VM On Item.database_name = VM.database_name
      and Item.Item_Dynamic_Class = VM.Item_Dynamic_Class
      and Item.Item_Object_identifier = VM.Item_Object_identifier
      and Item.Dynamic_Class = VM.Dynamic_Class
      and Item.Object_Identifier = VM.Object_Identifier
      Where L.loop_database_name='R_2016R3'
  ) as A
