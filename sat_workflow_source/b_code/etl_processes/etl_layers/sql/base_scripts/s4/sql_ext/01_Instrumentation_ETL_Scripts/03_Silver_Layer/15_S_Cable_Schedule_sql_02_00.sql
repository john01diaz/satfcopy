
-- cabinet to cabinet where we dont have cable connection will be extracted using below script.
CREATE OR REPLACE TEMP VIEW VW_Fabricated_Cable_Extract
As
Select Distinct
 Con.Database_name
,'LC_Connection' as Dynamic_Class
,Con.Object_Identifier
,Concat(Replace(Con.database_name,'_2016R3',''),'_',Con.Object_Identifier) as CableNumber
,From_Location as From_Location
,To_Location as To_Location
,'SHRH_CABLE_0000' as CatalogueNo
,0 as Length
,'As Built' as ProjectStatus	
,'' as Remarks	
,'' as Gland_From	
,'' as Gland_To	
,'' as Adapter_From	
,'' as Adapter_To	
,'Inst(Shared)' as Discipline
From sigraph_Silver.S_Connection Con
Where From_Location<>To_Location and Cable_Object_Identifier is null


