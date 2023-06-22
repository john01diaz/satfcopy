
-- Generate the groupings based on Row number and next row number.
CREATE OR REPLACE TEMP VIEW VW_Cable_Core_Group_Generated_Set
AS
Select 
A.object_identifier
,A.database_name
,A.Cable_Object_Identifier
,A.Core_Markings
,A.NoOfCore
,A.GroupType
,A.IsValidCableAndCore
,Group_Sequence
,Core_Sequence
,Case When ColorCount<=2 Or Derived_Group_Marking is not null
      Then Dense_Rank() Over(Partition by A.database_name,A.Cable_Object_Identifier order by A.RNT)
      Else 1 
      END as Group_Marking
   
From (
Select 
A.object_identifier
,A.database_name
,A.Cable_Object_Identifier
,A.Core_Markings
,A.NoOfCore
,A.GroupType
,A.IsValidCableAndCore
,A.RNT
,Group_Sequence
,Core_Sequence
,ColorCount
,Derived_Group_Marking
from VW_Cable_Core_Group_Prep_Query A
Where A.GroupType<>'Cores' and EvenRNT=1

UNION

Select 
A.object_identifier
,A.database_name
,A.Cable_Object_Identifier
,A.Core_Markings
,A.NoOfCore
,A.GroupType
,A.IsValidCableAndCore
,PR.RNT
,A.Group_Sequence
,A.Core_Sequence
,A.ColorCount
,A.Derived_Group_Marking
From VW_Cable_Core_Group_Prep_Query as A
-- Map the previous color object identifier to get their RNT to form the pair with same group number
Inner join VW_Cable_Core_Group_Prep_Query PR 
On PR.database_name=A.database_name 
and PR.EvenRNT=1 
and PR.object_identifier=A.Pair_Object_identifier
Where A.GroupType<>'Cores'  and A.EvenRNT=0


UNION

Select 
A.object_identifier
,A.database_name
,A.Cable_Object_Identifier
,A.Core_Markings
,A.NoOfCore
,A.GroupType
,A.IsValidCableAndCore
,1 as RNT
,Group_Sequence
,Core_Sequence
,ColorCount
,Derived_Group_Marking
from VW_Cable_Core_Group_Prep_Query A
Where A.GroupType='Cores'
) as A


