
-- Final Extracts
CREATE OR REPLACE TEMP View VW_Cable_Core_Extract
As
     
Select 
database_name
,object_identifier
,Cable_Object_Identifier
,Group_Marking
,GroupType
,Row_Number() Over(Partition by database_name,Cable_Object_Identifier,Core_Markings_Core_Type,Group_Marking
                   Order by database_name,cable_object_identifier,object_identifier--Group_Sequence,Core_Sequence,Core_Markings
                   ) as Group_Marking_Sequence
,Core_Markings
,Core_Markings_Core_Type
,IsValidCableAndCore
From (
Select
object_identifier
,database_name
,Cable_Object_Identifier
,Group_Marking
,Core_Markings
,NoOfCore
,GroupType
,IsValidCableAndCore
,Group_Sequence
,Core_Sequence
,'C' as Core_Markings_Core_Type
From VW_Cable_Core_Group_Generated_Set

UNION
-- Screen Cores
Select 
object_identifier
,database_name
,Cable_Object_Identifier
,Row_Number() Over(Partition by database_name,Cable_Object_Identifier
              Order by Core_Sequence
              ) as Group_Marking
,Core_Markings as Core_Markings
,NoOfCore
,GroupType
,IsValidCableAndCore
,Group_Sequence
,Core_Sequence
,'S' as Core_Markings_Core_Type
From VW_WireFunction
Where IsScreeningCore=1

UNION
-- Earth CORESs Extract

Select
object_identifier
,database_name
,Cable_Object_Identifier
,998 as Group_Marking
,'E' as Core_Markings
,NoOfCore
,GroupType
,IsValidCableAndCore
,Group_Sequence
,Core_Sequence
,'E' as Core_Markings_Core_Type
From VW_WireFunction
Where IsEarthCore=1

UNION
-- Shield CORESs Extract

Select
object_identifier
,database_name
,Cable_Object_Identifier
,999 as Group_Marking
,'OAS' as Core_Markings
,NoOfCore
,GroupType
,IsValidCableAndCore
,Group_Sequence
,Core_Sequence
,'OAS' as Core_Markings_Core_Type
From VW_WireFunction
Where IsOASH=1
) as A
order by 
database_name
,Core_Markings_Core_Type
,Cast(Group_Sequence as Bigint)
,CasT(Group_Marking as Bigint)
,Cast(Group_Marking_Sequence as Bigint)


