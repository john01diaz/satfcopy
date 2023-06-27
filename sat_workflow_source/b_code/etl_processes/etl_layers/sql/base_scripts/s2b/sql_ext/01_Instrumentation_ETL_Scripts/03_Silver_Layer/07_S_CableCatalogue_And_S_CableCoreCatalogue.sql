

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
,multipleReplace(Coalesce(LC_cable_my_construction_code,LC_item_complete_part_string),'{"NEU":"","~":"","_":"","ALT":"","?M":"µm"}') as Description
,Case When UPPER(Coalesce(LC_cable_my_construction_code,LC_item_complete_part_string)) like "%NEU%" Then "NEW" END as Cable_Standard
,LC_cable_color as Cable_Color
,PC_Part_def_manufacturer as Manufacturer
,regexp_extract(
      UPPER(REPLACE(Replace(Coalesce(LC_cable_my_construction_code,LC_item_complete_part_string),',','.'),'G','X')),
      '([0-9]+[ ]*[X][ ]*[0-9]+[X]?[0-9]*[,|.|//]?[0-9]*[.|//]?[0-9]*)'
      ) as CableDetails
,getCableCatalogue(Coalesce(LC_cable_my_construction_code,LC_item_complete_part_string),'OAS_Coll_SH') as OAS_Coll_SH
,getCableCatalogue(Coalesce(LC_cable_my_construction_code,LC_item_complete_part_string),'GSCR') as GSCR
,getCableCatalogue(Coalesce(LC_cable_my_construction_code,LC_item_complete_part_string),'Armoured') as Armoured
,getCableCatalogue(Coalesce(LC_cable_my_construction_code,LC_item_complete_part_string),'ArmourDescription') as ArmourDescription
from sigraph.Cable C
left outer join sigraph.Cable_eq CE 
ON CE.database_name=C.database_name 
and C.PC_Equipment_Item_Rel_href=CE.object_identifier
left outer join sigraph.Std_part_def CM 
ON CM.database_name=CE.database_name 
and CM.PC_Equipment_Part_def_Rel_dyn_class=CE.dynamic_class 
and CM.PC_Equipment_Part_def_Rel_href=CE.object_identifier
Where   UPPER(Coalesce(LC_cable_my_construction_code,LC_item_complete_part_string)) Not in ("REP DRAHT","RESERVIERT","FREI")
and UPPER(Coalesce(LC_cable_my_construction_code,LC_item_complete_part_string)) is not null
) as A


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
,Case When GroupTypeCNT * Cores= Count(Case When IsEarthCore=0 and IsScreeningCore=0 and IsOASH=0  
                                       Then Object_Identifier END) Over(Partition by database_name,Cable_Object_Identifier) 
      Then GroupType
      Else 'Cores'
      End as GroupType
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
        --UPPER(Coalesce(LC_wire_function_wire_spec,'')) like '%SL%'
        When UPPER(C.description) like '%PIMF%' and UPPER(TRIM(WF.LC_wire_function_wire_spec))='SC' Then 0
        When (UPPER(C.description) like '%PIMF%'  OR UPPER(C.description)='RE-2Y(ST)YV 24X2X0,8')
        and Translate(getColourRevised(UPPER(LC_wire_function_wire_spec)),'0,1,2,3,4,5,6,7,8,9,|,\,/,-','') ='SC'
        THEN 1
        ELSE 0 END  as IsScreeningCore  
,Case   When UPPER(Coalesce(LC_wire_function_wire_spec,'')) in ('SCHIRM','SCH','S','SH','SL','SC') 
        Then 1 
        ELSE 0 END IsOASH   
,case When LC_wire_function_wire_spec is not null 
      Then Row_Number() Over(Partition by WF.Database_name,WF.LC_item_functions_rel_href
                            ,getColourRevised(WF.LC_wire_function_wire_spec)
                        order by WF.Object_Identifier)
      Else 1                  
      END as Duplicate_CNT      
, Row_Number() Over(Partition by WF.Database_name,WF.LC_item_functions_rel_href 
               order by WF.database_name,WF.Object_Identifier) as Derived_Wire_Number
from sigraph.Wire_function WF
Inner join VW_Cable C 
ON  WF.database_name = C.database_name 
and WF.LC_item_functions_rel_href=C.object_identifier
) as A
) as A



 CREATE OR REPLACE TEMP VIEW VW_Cable_Core_Group_Prep_Query
 As
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
 ,Case When Coalesce(GroupType,'Cores')='Pairs' Then 1
       END as Derived_Group_Marking
 ,Coalesce(GroupType,'Cores') as GroupType
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



CREATE OR REPLACE TEMP View VW_Cable_Core_Extract
As
Select
database_name
,object_identifier
,Cable_Object_Identifier
,Case When GroupType='Cores' and Core_Markings_Core_Type='C' 
      Then Group_Marking_Sequence Else Group_Marking END as Group_Marking
,GroupType
,Case When GroupType='Cores' and Core_Markings_Core_Type='C'
      Then 1 Else Group_Marking_Sequence END as Group_Marking_Sequence
,Core_Markings
,Core_Markings_Core_Type
,IsValidCableAndCore
From (
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
) as A


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
,Coalesce(UPPER(Cable_Color),'BK/BL') as Colour1
,'' as Colour2
,'TRUE' as AllowUse
,'' as DrumLength
,'' as LineType
,'' as LineTypeColor
,'' as LineTypeWidth
,'' as LineTypeArrowHead
,Case when IsValidCableAndCore=0 then 'Special Cable'
      when A.Cores<>1 and B.GroupType<>'Cores' and CountGroupMarkings=1 then 'Special Cable'
      when A.GroupType<>B.GroupType Then 'Special Cable'
       END as Remarks
from VW_Cable A
Inner join (
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


 df1=spark.sql('Select * from VW_Cable_Catalogue_Extract where database_name == "R_2016R3"')

 dbutils.fs.rm("dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_CableCatalogue",True)

 df1 = cleansing_df(df1)

 df1.write.save(
     format = "delta"
    ,mode   = "overwrite"
    ,path   = 'dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_CableCatalogue'
 )


 df2 = spark.sql("""Select * from VW_Cable_Core_Extract 
                     where database_name == "R_2016R3"
                     order by database_name,Cable_Object_Identifier,Group_Marking,Group_Marking_Sequence"""
                 )

 dbutils.fs.rm('dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_CableCoreCatalogue',True)

 df2 = cleansing_df(df2)

 df2.write.save(
     format = 'delta'
     ,mode  = 'overwrite'
     ,path  = 'dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_CableCoreCatalogue'
     ,overwriteSchema = True
 )
