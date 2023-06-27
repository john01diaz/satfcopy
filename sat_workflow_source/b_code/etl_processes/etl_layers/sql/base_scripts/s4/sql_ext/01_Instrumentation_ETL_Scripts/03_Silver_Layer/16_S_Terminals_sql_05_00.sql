
Create Or Replace Temp View VW_Terminals
As
Select
database_name
,dynamic_class
,object_identifier
,Parent_Equipment_No
,Equipment_No
,Marking
,Dense_Rank() Over(Partition by Parent_Equipment_No,Equipment_No order by Cast(Marking as Bigint),Marking) as Sequence
,Class
From (
Select * from VW_Terminals_Prep_1
UNION
Select * from VW_Terminals_Prep_2
UNION
Select * from VW_Terminals_Prep_3
UNION
Select * from VW_Terminals_Prep_4
) as A
-- Restrict the terminal data only for those components, which are loaded in Component and Instrument loaders.
LEFT SEMI JOIN (
Select 
TagNo as Equipment_No 
from Sigraph_Silver.S_Instrument_Index
UNION
Select Equipment_No 
from Sigraph_Silver.S_Component
) as RD 
ON RD.Equipment_No=A.Equipment_No

