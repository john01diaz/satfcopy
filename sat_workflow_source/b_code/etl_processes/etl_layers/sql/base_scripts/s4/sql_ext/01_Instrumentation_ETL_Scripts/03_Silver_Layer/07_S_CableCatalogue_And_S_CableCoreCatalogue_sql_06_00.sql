
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

