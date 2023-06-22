
-- Cable Catalogue Extract
 
Select 
CatalogueNo
,Manufacturer
,Class
,Description
,GroupType
,NoOfGroups
,Armoured
,OAScr
,GroupScr
,EarthCore
,Voltage
,Size
,Earth_Core_Size
,OD
,Material
,Colour1
,Colour2
,AllowUse
,DrumLength
,LineType
,LineTypeColor
,LineTypeWidth
,LineTypeArrowHead
,Remarks
From (
Select C.*
,CM.CatalogueNo
,Row_Number() Over(partition by CM.CatalogueNo order by CM.Cable_Object_Identifier) as RNT
from sigraph_silver.S_CableCatalogue  C
Inner join sigraph_silver.S_CableCatalogueNumber_Master CM ON CM.database_name=C.database_name 
and CM.Cable_Object_Identifier = C.Object_Identifier
Where Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')
and C.database_name in (Select Database_name from VW_Database_names)
) as A 
Where RNT=1
