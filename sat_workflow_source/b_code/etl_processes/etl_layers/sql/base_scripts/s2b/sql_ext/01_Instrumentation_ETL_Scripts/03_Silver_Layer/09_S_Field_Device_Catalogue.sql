

Create Or Replace Temp View VW_Instrument_Identification
As
With CTE 
 (loop_element_database_name
,loop_element_dynamic_class
,loop_element_Object_Identifier
,Type
)
As (
Select 
A.loop_element_database_name
,A.loop_element_dynamic_class
,A.loop_element_Object_Identifier
,1 as Type
from Sigraph.Loop_Elements A 
Inner join Sigraph_Silver.S_ItemFunction B 
On  A.Loop_Element_Database_Name     = B.database_name
and A.loop_element_dynamic_class     = B.loop_element_dynamic_class
and A.loop_element_Object_Identifier = B.loop_element_Object_Identifier
Where CS_Loop_CS_Loop_element_dyn_class = 'CS_Loop_spez' 
and B.Type='Field Device'
and A.LC_Item_function_CS_Loop_element_dyn_class is not null 
)

Select * from CTE

UNION
Select 
A.loop_element_database_name
,A.loop_element_dynamic_class
,A.loop_element_Object_Identifier
,2 as Type
from Sigraph.Loop_Elements A 

LEFT ANTI JOIN CTE B ON A.loop_element_database_name=B.loop_element_database_name
and A.loop_element_dynamic_class=B.loop_element_dynamic_class 
and A.loop_element_Object_Identifier=B.loop_element_Object_Identifier

left outer join sigraph_Reference.DeviceType as DTC ON  UPPER(TRIM(DTC.SigraphDeviceType))=UPPER(TRIM(A.CS_device_type)) 

Where CS_Loop_CS_Loop_element_dyn_class='CS_Loop_spez' 
and A.LC_Item_function_CS_Loop_element_dyn_class is null
and Case When CS_location_full_designation is null Or UPPER(CS_device_type) like '%INDUCTIVE%SENSOR%' Then 1 else 0 End=1
and loop_element_dynamic_class not in ('CS_Loop_element_hw_bi'
,'CS_Loop_element_hw_bo'
,'CS_Loop_element_hw_ai'
,'CS_Loop_element_hw_ao')
and TRIM(
       Coalesce(Replace(Replace(Replace(Replace(RevisedDeviceType,'ANALOG INPUT','AI'),'ANALOG OUTPUT','AO')
                 ,'DIGITAL INPUT','DI'),'DIGITAL OUTPUT','DO')
        ,A.CS_device_type,'')
        ) 
Not in 
 ('C300 DO','DCS AI','FTA DO','IOTA AI','PLC DO','PLS AO','PLS DI','PLS DO','PLS OUTBOUND','SM AI','SM DI','SM DO','SPS AI','SPS DI')


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


 dbutils.fs.rm('dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Field_Device_Catalogue', True)

 DF=spark.sql('Select * from VW_FieldDevice_PrepQuery where database_name == "R_2016R3"')

 DF.write.save(
     path            = 'dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Field_Device_Catalogue'
    ,format          = 'delta'
    ,mode            = 'overwrite'
   ,overwriteSchema = True
 )
