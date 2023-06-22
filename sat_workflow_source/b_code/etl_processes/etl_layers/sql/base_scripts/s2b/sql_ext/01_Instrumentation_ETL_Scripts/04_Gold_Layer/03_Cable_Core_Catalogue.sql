


Select 
CM.CatalogueNo
,CM.Description
,CM.Group_Marking
,CM.Group_Marking_Sequence
,CM.Core_Markings
,CM.Core_Markings_Core_Type
From (
Select
C.*
,CC.Description
,CM.CatalogueNo
,Dense_Rank() Over(Partition by CM.CatalogueNo order by CM.database_name,CM.Cable_Object_Identifier) as RNT
From sigraph_silver.S_CableCoreCatalogue C
Inner join sigraph_silver.S_CableCatalogueNumber_Master CM 
ON CM.database_name=C.database_name 
and CM.Cable_Object_Identifier = C.Cable_Object_Identifier
Inner join sigraph_silver.S_CableCatalogue CC 
ON CC.database_name=C.database_name 
and CC.Object_Identifier = C.Cable_Object_Identifier
Where CC.Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')
and  C.database_name in (Select Database_name from VW_Database_names)
) as CM

Where RNT=1
