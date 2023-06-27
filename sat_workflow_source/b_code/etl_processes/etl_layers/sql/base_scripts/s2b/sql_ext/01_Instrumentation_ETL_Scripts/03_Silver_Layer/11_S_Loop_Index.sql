

CREATE OR REPLACE TEMP VIEW LOOP_INDEX
AS
Select distinct
loop_database_name
,loop_dynamic_class
,loop_object_identifier
,name
,case when 
        CS_loop_id like '%*'  or CS_loop_id like '%?'  or CS_loop_id like '%~' 
        then translate(CS_loop_id,"*?~","") else CS_loop_id 
  end as LoopNo
,regexp_extract(CS_loop_id,('^[0-9]+'),0) as Area
,regexp_extract(CS_loop_id,"(^[0-9]+)([A-Z]+)([0-9]+)",3) as  Number
,regexp_extract(CS_loop_id,"(^[0-9]+)([A-Z]+)([0-9]+)([-_ ]*)([A-Z0-9]*$)",5) as  Suffix
,to_get_loop_format_tag(CS_loop_id) as FormatName
,concat_ws('-',PBS.Site_Code,Coalesce(PBS.Engineering_Plant_Code,PBS.Plant_Code)
            ,Coalesce(PBS.Engineering_Process_unit,PBS.Process_Unit)) as AreaPath

,CS_loop_kat_id as Function
,loop_CS_comment as Remarks
,translate(CS_loop_description_1,"?#*","") as  `Loop_Service_1`
,CS_function_description as `Func_Description`
,SAP_responsible_work_center as `Resp_Work_Center`
,CS_air_distributor as `Air_Distributor`
,CS_variable_part_drawing_number as `Var_Part_of_Drawing_No`
,CS_loop_id_1 as `Suppl_char_1`
,CS_visual_inspection as `Visual_Inspection`
,CS_y_coordinate_pi_data as `Y_Coord`
,CS_et_node_path as `ET_structure`
,CS_x_coordinate_pi_data as `X_Coord`
,CS_field_distributor_output_id as `Field_Distrib_Output`
,CS_loop_id_3 as `Suppl_char_3`
,CS_loop_id_2 as `Suppl_char_2`
,SAP_planning_group as `Planning_Group`
,CS_tested_by as `Tested_by`
,CS_test_acc_to_test_catalog as `Test_Acc_to_Test_Catalog`
,CS_test_acc_by_test_catalog as `Test_Acc_by_Test_Catalog`
,CS_logic_diagram_typical as `Logic_Diag_Typical`
,CS_inspection_purpose as `Purpose_of_Inspection`
,CS_inspection_interval as `Inspection_Interval`
,loop_CS_loop_unit as `Unit`
,CS_accompanying_documents as `Accomp_Documents`
,CS_field_distributor_input_id as `Field_Distrib_Input`
,CS_realization_position as `Realization_Pos`
,CS_function_test as `Function_Test` 
,CS_loop_description_3 as `Loop_Service_3`
,CS_graphic_typical as `Graphical_Typical`
,CS_standard_loop_id as `Standard_Loop_ID`
,CS_loop_description_2 as `Loop_Service_2`
,CS_loop_status_remark as `Status_Remark`
,CS_loop_lfd_nr as  `Loop_No`
,CS_loop_function as `Loop_Function`
,Case When Loop_Dynamic_Class='CS_Loop_spez' Then 'Instrumentation' Else 'Electrical' End as Class
,Coalesce(get_loop_picklist_display_value(array('CS_loop_status',CS_loop_status)),'BEING PLANNED') as Status
,get_loop_picklist_display_value(array('CS_source',CS_source)) as `EP_Origin`
,get_loop_picklist_display_value(array('loop_CS_classification',loop_CS_classification)) as `Classification`
,get_loop_picklist_display_value(array('CS_related_report',CS_related_report)) as  `Related_Report`
,get_loop_picklist_display_value(array('CS_root_extraction_point',CS_root_extraction_point)) as `Root_Extraction_Point`
,get_loop_picklist_display_value(array('CS_special_requirements_1',CS_special_requirements_1)) as `Special_Req_1`
,get_loop_picklist_display_value(array('CS_special_requirements_2',CS_special_requirements_2)) as `Special_Req_2`
,get_loop_picklist_display_value(array('CS_special_requirements_3',CS_special_requirements_3)) as `Special_Req_3`
,get_loop_picklist_display_value(array('loop_CS_classification_by',loop_CS_classification_by)) as `Classification_by`
,get_loop_picklist_display_value(array('CS_sifpro_relevant',CS_sifpro_relevant)) as `SIFpro_Relevant`
from  (
      Select *
      --Plant_Code
      ,Coalesce(Replace(Replace(Replace(Replace(l.CS_production_unit,'-',''),'MMP4','MMP64'),'MMP6','MMP64'),'MMP644','MMP64'),'') as Plant_Code      
      -- Process_unit
      ,Replace(TRIM(
      Case When substring(l.name,1,Charindex('_',l.name)-1)='RAUM' Then substring(l.name,Charindex('_',l.name)+1,200) 
       When l.name like 'OFEN%' Then l.name Else substring(l.name,Charindex('_',l.name)+1,200) 
      End),' ','_') as Process_unit
      from sigraph.Layer l
      )l

inner join  Sigraph.Loop  
on  l.database_name      == loop.Loop_database_name
and l.object_identifier  == loop.Layer_CS_Loop_href
and l.dynamic_class      == loop.Layer_CS_Loop_dyn_class

inner join  Sigraph_Reference.PlantBreakDown pbs
ON    l.Plant_Code = PBS.Plant_Code
AND   l.Process_Unit = PBS.Process_Unit
AND   regexp_extract(loop.CS_loop_id,('^[0-9]+'),0) = PBS.Area 

where Template_loop == "FALSE"



 dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Loop_Index",True)

 df = (
     spark.sql("select * from Loop_Index where loop_database_name == 'R_2016R3'")
     .withColumnRenamed('loop_database_name', 'database_name')
     .withColumnRenamed('loop_dynamic_class', 'dynamic_class')
     .withColumnRenamed('loop_object_identifier', 'object_identifier')
 )

 df = cleansing_df(df)

 df.write.save(
     format = "delta"
    ,mode   = "overwrite"
    ,path   = "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Loop_Index"  
    ,overwriteSchema = True
 )
