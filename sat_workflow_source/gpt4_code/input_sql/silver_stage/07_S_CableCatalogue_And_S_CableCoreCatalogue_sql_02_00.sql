
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
) as A

