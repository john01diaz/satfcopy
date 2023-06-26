
Create OR Replace TEMP View VW_Cable
As
Select 
Manufacturer        
,Description
,Cable_Color
,Case When Coalesce(CableDetails,'')='' Then Description Else CableDetails END as CableDetails
,Split(CableDetails,'X')[0] as Cores
,Cast(Case When regexp_count(CableDetails,'X') =2 Then Split(CableDetails,'X')[1]  END as Int) as CableGroup
,Split(Reverse(substring(Reverse(CableDetails),1,Instr(Reverse(CableDetails),'X')-1)),'/')[0] as Size
,Split(Reverse(substring(Reverse(CableDetails),1,Instr(Reverse(CableDetails),'X')-1)),'/')[1] as Earth_Core_Size
,Case When regexp_count(CableDetails,'X') =2 and Split(CableDetails,'X')[1]=2 Then 'Pairs'
      When regexp_count(CableDetails,'X') =2 and Split(CableDetails,'X')[1]=3 Then 'Triads'
      When regexp_count(CableDetails,'X') =2 and Split(CableDetails,'X')[1]=4 Then 'Quads'
      ELSE 'Cores'
      END as GroupType
 ,Case When regexp_count(CableDetails,'X') =2 and Split(CableDetails,'X')[1]=2 Then 2
      When regexp_count(CableDetails,'X') =2 and Split(CableDetails,'X')[1]=3 Then 3
      When regexp_count(CableDetails,'X') =2 and Split(CableDetails,'X')[1]=4 Then 4
      ELSE 1
      END as GroupTypeCNT      
,OAS_Coll_SH
,GScr
,Armoured
,ArmourDescription
,Cable_Standard
,object_identifier
,database_name
From (
Select Distinct
C.database_name
,C.object_identifier
,multipleReplace(Coalesce(LC_item_complete_part_string,LC_cable_my_construction_code),'{"NEU":"","~":"","_":"","ALT":"","?M":"µm"}') as Description
,Case When UPPER(Coalesce(LC_item_complete_part_string,LC_cable_my_construction_code)) like "%NEU%" Then "NEW" END as Cable_Standard
,LC_cable_color as Cable_Color
,PC_Part_def_manufacturer as Manufacturer
-- Calculated Columns
,regexp_extract(
      UPPER(REPLACE(Replace(Coalesce(LC_item_complete_part_string,LC_cable_my_construction_code),',','.'),'G','X')),
      '([0-9]+[ ]*[X][ ]*[0-9]+[X]?[0-9]*[,|.|//]?[0-9]*[.|//]?[0-9]*)'
      ) as CableDetails
,getCableCatalogue(Coalesce(LC_item_complete_part_string,LC_cable_my_construction_code),'OAS_Coll_SH') as OAS_Coll_SH
,getCableCatalogue(Coalesce(LC_item_complete_part_string,LC_cable_my_construction_code),'GSCR') as GSCR
,getCableCatalogue(Coalesce(LC_item_complete_part_string,LC_cable_my_construction_code),'Armoured') as Armoured
,getCableCatalogue(Coalesce(LC_item_complete_part_string,LC_cable_my_construction_code),'ArmourDescription') as ArmourDescription
from sigraph.Cable C
left outer join sigraph.Cable_eq CE 
ON CE.database_name=C.database_name 
and C.PC_Equipment_Item_Rel_href=CE.object_identifier
left outer join sigraph.Std_part_def CM 
ON CM.database_name=CE.database_name 
and CM.PC_Equipment_Part_def_Rel_dyn_class=CE.dynamic_class 
and CM.PC_Equipment_Part_def_Rel_href=CE.object_identifier
Where   UPPER(Coalesce(LC_item_complete_part_string,LC_cable_my_construction_code)) Not in ("REP DRAHT","RESERVIERT","FREI")
and UPPER(Coalesce(LC_item_complete_part_string,LC_cable_my_construction_code)) is not null
) as A;

Create OR Replace TEMP View VW_WireFunction
As
Select Distinct
Database_name
,Object_identifier
,Cable_Object_Identifier
,Core_Markings
,IsEarthCore
,IsScreeningCore
,IsOASH
,Group_Sequence
,Core_Sequence
,FindGroupType(Color_Marking) as GroupType
,NoOfCore
,IsValidCableAndCore
,Color_Marking
From (
Select Distinct
Database_name
,Object_identifier
,Cable_Object_Identifier
,Case When Duplicate_CNT>1 -- If duplicate core for same cable, add the running number at the end.
      Then Concat(getColourRevised(Core_Markings),Cast(Duplicate_CNT as string)) 
      Else getColourRevised(Core_Markings) END as Core_Markings
,IsEarthCore
,IsScreeningCore
,IsOASH
,Coalesce(Cast(REGEXP_EXTRACT(Core_Markings,"([0-9]+)",1) as Bigint),0) as Group_Sequence
,Coalesce(Cast(
    coalesce(
    NULLIF(REGEXP_EXTRACT(Core_Markings,"([0-9]*)([|A-Za-z]*)([0-9]*)",3),''),REGEXP_EXTRACT(Core_Markings,"([0-9]+)",1)
            )
    as Bigint),0) as Core_Sequence
-- derived columns
-- Check whether the cable description and wire function cores count is matching? if yes then consider cores from cable, else consider the count cores from wire function and update the cable cores.
,Case When GroupTypeCNT * Cores= Count(Case When IsEarthCore=0 and IsScreeningCore=0 and IsOASH=0  
                                       Then Object_Identifier END) Over(Partition by database_name,Cable_Object_Identifier) 
      Then GroupType
      Else 'Cores'
      End as GroupType
-- Check whether the cable description and wire function cores count is matching? if yes then consider cores from cable, else consider the count cores from wire function and update the cable cores.
,Case When GroupTypeCNT * Cores= Count(Case When IsEarthCore=0 and IsScreeningCore=0 and IsOASH=0  
                                       Then Object_Identifier END) Over(Partition by database_name,Cable_Object_Identifier) 
      Then Cores
      Else Count(Case When IsEarthCore=0 and IsScreeningCore=0 and IsOASH=0  
                                       Then Object_Identifier END) Over(Partition by database_name,Cable_Object_Identifier)
      End as NoOfCore
    
,Case When GroupTypeCNT * Cores= Count(Case When IsEarthCore=0 and IsScreeningCore=0 and IsOASH=0  
                                       Then Object_Identifier END) Over(Partition by database_name,Cable_Object_Identifier) 
      Then 1
      Else 0
      End as IsValidCableAndCore  
,to_get_sort(collect_set(substring(Translate(getColourRevised(Core_Markings),'0,1,2,3,4,5,6,7,8,9,|,\,/,-',''),1,2)) 
            over(partition by A.database_name,A.Cable_Object_Identifier)) as Color_Marking
From (     
Select Distinct
WF.Database_name
,WF.Object_identifier
,WF.LC_item_functions_rel_href as Cable_Object_Identifier
,C.GroupTypeCNT
,C.Cores
,C.GroupType
-- Derived Columns

,Case 	When Description="RE-2Y(ST)YV PIMF 24X2X1,3" And LC_wire_function_wire_spec is null  
        Then "WS16"
        When Description='HONEYWELL J19 50X0,5 GRAU' and lower(LC_wire_function_wire_spec)='ge/gn' 
        Then 'ge/gn1'
        When LC_wire_function_wire_spec is null
        Then Row_Number() Over(Partition by WF.Database_name,WF.LC_item_functions_rel_href 
               order by WF.database_name,WF.Object_Identifier)
        ELSE LC_wire_function_wire_spec END as Core_Markings
        
,Case When Translate(lower(Coalesce(LC_wire_function_wire_spec,'')),'/,\\','') in ('gegn','gnge','yegn','gnye')
     Then 1 
     Else 0
     END as IsEarthCore
   
,Case   When  UPPER(Coalesce(LC_wire_function_wire_spec,''))  like '%SCHWARZ%' 
        THEN 0
        When  UPPER(Coalesce(LC_wire_function_wire_spec,'')) like '%SL%'
          OR  UPPER(Coalesce(LC_wire_function_wire_spec,'')) like '%SH%'
          OR  UPPER(Coalesce(LC_wire_function_wire_spec,'')) like '%SC%'
        THEN 1
        ELSE 0 END  as IsScreeningCore  
        
,Case   When  UPPER(Coalesce(LC_wire_function_wire_spec,'')) in ('SCHIRM','SCH','S','SH') 
        Then 1 
        ELSE 0 END IsOASH   
-- Duplicate core for same cable, then assign running number at the end. 
,case When LC_wire_function_wire_spec is not null 
      Then Row_Number() Over(Partition by WF.Database_name,WF.LC_item_functions_rel_href
                            ,getColourRevised(WF.LC_wire_function_wire_spec)
                        order by WF.Object_Identifier)
      Else 1                  
      END as Duplicate_CNT      
-- generate the running number per cable oid. As few of the wire core markings are null.
, Row_Number() Over(Partition by WF.Database_name,WF.LC_item_functions_rel_href 
               order by WF.database_name,WF.Object_Identifier) as Derived_Wire_Number
from sigraph.Wire_function WF
Inner join VW_Cable C 
ON  WF.database_name = C.database_name 
and WF.LC_item_functions_rel_href=C.object_identifier

) as A
) as A;

-- MAGIC %sql

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
 ) as A;

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
) as A;

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
,Cast(Group_Marking_Sequence as Bigint);

-- Cable Catalogue Extract
Create OR REPLACE TEMP VIEW VW_Cable_Catalogue_Extract
AS
Select Distinct
A.Database_name
,A.Object_Identifier
,Manufacturer
,Case When Cast(Size as Decimal(18,2)) >2.5  
      Then 'Elec(Shared)'
      When Cast(Size as Decimal(18,2)) between 1 and 2.5 
      Then 'Inst(Shared)'
      ELSE 'Instrumentation'
      END as Class
-- New Cable Vs Old Cable Logic
,Concat(A.Description,' ',Coalesce(CS.Standard,Cable_Standard,'')) as Description
,B.GroupType   
,Cast(Case 
      When B.GroupType='Pairs' Then A.Cores
      ELSE NumberOfCore 
      END as Bigint) as NoOfGroups
,A.Armoured
,B.OAScr
,B.GroupScr
,B.EarthCore
,'' as Voltage
,Concat(Replace(Size,'0.8','0.5'),'mm2') as Size
,Case When B.EarthCore='TRUE' 
      Then Coalesce(Concat(Replace(Earth_Core_Size,'0.8','0.5'),'mm2'),Concat(Replace(Size,'0.8','0.5'),'mm2')) 
      END as Earth_Core_Size
,1 as OD
,Case When A.Description like '%A' then 'A' else 'Cu' END as Material
,Coalesce(UPPER(Cable_Color),'BK_And_BL') as Colour1
,'' as Colour2
,'TRUE' as AllowUse
,'' as DrumLength
,'' as LineType
,'' as LineTypeColor
,'' as LineTypeWidth
,'' as LineTypeArrowHead
,Case when IsValidCableAndCore=0 then 'Special Cable'
      when A.Cores<>1 and B.GroupType<>'Cores' and CountGroupMarkings=1 then 'Special Cable' END as Remarks
from VW_Cable A
Inner join (
-- Cable Core Details --> updating into cable.
Select
B.Database_name
,B.Cable_Object_Identifier
,B.GroupType
,Count(Distinct Case when Core_Markings_Core_Type not in ('E','OAS','S') Then Group_Marking END) as CountGroupMarkings
,Count(Case when Core_Markings_Core_Type='C' Then Core_markings END) as NumberOfCore
,MAX(Case when Core_Markings_Core_Type='E' Then 'TRUE' ELSE 'FALSE' END) as EarthCore
,MAX(Case when Core_Markings_Core_Type='OAS' Then 'TRUE' ELSE 'FALSE'  END) as OAScr
,MAX(Case when Core_Markings_Core_Type='S' Then 'TRUE' ELSE 'FALSE'  END) as GroupScr
,MAX(IsValidCableAndCore) as IsValidCableAndCore
,MAX(Core_Marking_Groups) as Core_Marking_Groups
From VW_Cable_Core_Extract B
Left outer join (
Select 
database_name
,object_identifier
,concat_ws(',',collect_list(Core_Markings) Over(partition by database_name,Cable_Object_Identifier)) as Core_Marking_Groups
From VW_Cable_Core_Extract 
Where Core_Markings_Core_Type='C'
) as C On B.database_name=C.database_name and C.Object_Identifier=B.Object_Identifier
Group by B.Database_name,B.Cable_Object_Identifier,B.GroupType
) as B On A.database_name=B.database_name and A.Object_Identifier=B.Cable_ObjecT_Identifier
-- Cable Standard Categorization
Left outer Join Sigraph_reference.Cable_Standard_Categorization CS ON 
CS.Type=Case When A.Description like '%NYM-J%'     Then 'NYM-J'
             When A.Description like '%NYY-J%'	   Then 'NYY-J' 
             When A.Description like '%NYM-O%'     Then 'NYM-O'
             When A.Description like '%NYY-O%'     Then 'NYY-O'
             When A.Description like '%NYCWY%'     Then 'NYCWY'
             When A.Description like '%NYCY%'      Then 'NYCY'
             When A.Description like '%H07RN-F%'   Then 'H07RN-F'
             When A.Description like '%H03VV-F%'   Then 'H03VV-F'
             When A.Description like '%H05VV-F%'   Then 'H05VV-F'
         END
and CS.Cores=A.Cores
and CS.Color=B.Core_Marking_Groups
and CS.IsEarthCore=B.EarthCore