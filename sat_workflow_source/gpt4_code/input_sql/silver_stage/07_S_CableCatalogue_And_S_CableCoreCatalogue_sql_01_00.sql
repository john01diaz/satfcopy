
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
) as A

