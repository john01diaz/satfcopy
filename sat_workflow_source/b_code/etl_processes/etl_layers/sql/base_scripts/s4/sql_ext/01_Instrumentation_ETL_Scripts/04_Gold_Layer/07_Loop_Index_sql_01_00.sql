
Select distinct
Area
,LoopNo as `Loop No`
,Function
,Number
,Suffix
,Loop_Service_1 as `Loop Service-1`
,Status
,"TRUE" as Wired
,"TRUE" as Drawing
,Remarks
,"ILP" as ClassName
,FormatName
,AreaPath
,Loop_Function as `Loop Function`
,Loop_Service_2  As `Loop Service-2`
,Loop_Service_3  As `Loop Service-3`
,SIFpro_Relevant  As `SIFpro Relevant`
,Func_Description  As `Func Description`
,Resp_Work_Center  As `Resp Work Center`
,Air_Distributor  As `Air Distributor`
,Var_Part_of_Drawing_No  As `Var Part of Drawing No`
,Classification_by  As `Classification by`
,Suppl_char_1  As `Suppl char 1`
,Visual_Inspection  As `Visual Inspection`
,EP_Origin  As `EP Origin`
,`Y_Coord.`  As `Y Coord.`
,ET_structure  As `ET structure`
,Classification  As `Classification`
,Related_Report  As `Related Report`
,`X_Coord.`  As `X Coord.`
,Field_Distrib_Output  As `Field Distrib Output`
,Root_Extraction_Point  As `Root Extraction Point`
,Suppl_char_3  As `Suppl char 3`
,Suppl_char_2  As `Suppl char 2`
,Planning_Group  As `Planning Group`
,Tested_by  As `Tested by`
,Test_Acc_to_Test_Catalog  As `Test Acc to Test Catalog`
,Test_Acc_by_Test_Catalog  As `Test Acc by Test Catalog`
,Logic_Diag_Typical  As `Logic Diag Typical`
,Purpose_of_Inspection  As `Purpose of Inspection`
,Inspection_Interval  As `Inspection Interval`
,Special_Req_3  As `Special Req 3`
,Unit  As `Unit`
,Accomp_Documents  As `Accomp Documents`
,Special_Req_2  As `Special Req 2`
,Field_Distrib_Input  As `Field Distrib Input`
,Special_Req_1  As `Special Req 1`
,Realization_Pos  As `Realization Pos`
,Function_Test  As `Function Test`
,Graphical_Typical  As `Graphical Typical`
,Standard_Loop_ID  As `Standard Loop ID`
,Status_Remark  As `Status Remark`
FROM sigraph_silver.S_LOOP_INDEX loop 
Where Class='Instrumentation' and database_name in (Select Database_name from VW_Database_names) 
