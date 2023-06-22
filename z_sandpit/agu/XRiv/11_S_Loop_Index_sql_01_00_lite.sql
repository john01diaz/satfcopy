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

                  ---- **** Plant breakdown structure columns ****----
,concat_ws('-',Site_Code,Coalesce(Revised_Plant_Code,Plant_Code),Coalesce(Revised_Process_Unit,Process_Unit)) as AreaPath
                  --------------------------------------------------
,CS_loop_kat_id as Function
,loop_CS_comment as Remarks
,translate(CS_loop_description_1,"?#*","") as  `Loop_Service_1`
,CS_function_description as `Func_Description`
,SAP_responsible_work_center as `Resp_Work_Center`
,CS_air_distributor as `Air_Distributor`
,CS_variable_part_drawing_number as `Var_Part_of_Drawing_No`
,CS_loop_id_1 as `Suppl_char_1`
,CS_visual_inspection as `Visual_Inspection`
,CS_y_coordinate_pi_data as `Y_Coord.`
,CS_et_node_path as `ET_structure`
,CS_x_coordinate_pi_data as `X_Coord.`
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
-- Picklist Property
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
from  sigraph.Layer l

inner join Sigraph.Loop loop
on  l.database_name      == loop.Loop_database_name
and l.object_identifier  == loop.Layer_CS_Loop_href
and l.dynamic_class      == loop.Layer_CS_Loop_dyn_class

INNER JOIN Sigraph_Reference.PlantBreakDown PBS ON
PBS.Process_Unit=Case When L.Name like 'RAUM%' Then Substring(Replace(L.name,'RAUM_',''),1,100)
                      When Substring(L.name,1,charindex('_',L.Name )-1)=CS_loop_unit
                      Then Substring(L.name,charindex('_',L.Name )+1,100)
                      Else Substring(L.name,1,charindex('_',L.Name )-1)
                  END
AND
PBS.Area_Code=regexp_extract(CS_loop_id,('^[0-9]+'),0)
--inner join sigraph_reference.PlantbreakdownStructure pbs on l.name == pbs.distinct_column


where Template_loop == "FALSE"