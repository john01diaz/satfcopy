
Select Distinct
CableNumber
,Core_Markings
,Parent_Equipment_No
,Equipment_No
,Marking
,Left_Right
From Sigraph_Silver.S_Terminations
Where Class in ('Instrumentation','Inst(Shared)','Elec(Shared)') 
and database_name in (Select Database_name from VW_Database_names)