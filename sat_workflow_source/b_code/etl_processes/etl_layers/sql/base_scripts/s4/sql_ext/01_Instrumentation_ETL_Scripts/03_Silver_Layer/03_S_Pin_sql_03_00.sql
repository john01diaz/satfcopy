
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
From VW_EL_Pin



