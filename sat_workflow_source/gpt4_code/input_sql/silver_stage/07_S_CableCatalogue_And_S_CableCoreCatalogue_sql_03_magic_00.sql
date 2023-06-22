

-- MAGIC
CREATE OR REPLACE TEMP VIEW VW_Cable_Core_Group_Prep_Query
As
-- Non Earth Records
Select 
*
, Coalesce(
          Lag(A.object_identifier) over(Partition by A.database_name,A.Cable_Object_Identifier 
                                            order by A.object_identifier)
          ,Case When EvenRNT=1 and RNT=1 then object_identifier END) 
          as Pair_object_identifier
From (
Select
A.object_identifier
,A.database_name
,A.Cable_Object_Identifier
,NoOfCore
,Core_Markings
,IsValidCableAndCore
,Group_Sequence
,Core_Sequence
,ColorCount
,RNT
,Color_Marking
-- Get those 8 colors which Discipline highlighted, if the combination present in this then generate the Pair
,Case When Coalesce(GroupType,'Cores')='Pairs' Then 1
      END as Derived_Group_Marking
,Coalesce(GroupType,'Cores') as GroupType
-- In Sigraph the object identifier is stored based on the groupings. To derive the group the we need to divide the groupings based object identifier. Map the first and second object identifier if it is paired.
,Case When Coalesce(GroupType,'Cores')<>'Cores' 
      Then MOD(Row_Number() over(Partition by A.database_name,A.Cable_Object_Identifier
                                 order by object_identifier),2)   
ELSE 1 END as EvenRNT
From (
Select 
A.object_identifier
,A.database_name
,A.Cable_Object_Identifier
,NoOfCore
,GroupType
,Core_Markings
,IsValidCableAndCore
,Group_Sequence
,Core_Sequence
,C.ColorCount
,Color_Marking
 ,Row_Number() over(Partition by A.database_name,A.Cable_Object_Identifier 
                   order by Group_Sequence,Core_Sequence,Core_Markings,object_identifier) as RNT
 from VW_WireFunction A
-- Get the color count per cable, if the count is less than or equal to 2, then apply the pair logic. if more then use those 8 color logic mentioned in above script rest all populate as core with single group- Discipline approved.
Inner join (
Select
database_name
,Cable_Object_Identifier
,Count(Distinct Color_Marking) as ColorCount
From VW_WireFunction
Group by database_name,Cable_Object_Identifier
) as C ON A.database_name=C.database_name
and A.Cable_Object_Identifier=C.Cable_Object_Identifier
Where IsEarthCore=0 and IsScreeningCore=0 and IsOASH=0
) as A
) as A

