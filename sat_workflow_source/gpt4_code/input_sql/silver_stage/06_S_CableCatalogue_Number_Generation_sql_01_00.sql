
Create Or Replace Temp View VW_CatalogueNo
As

Select 
Concat('SHRH_CABLE_',2000+Dense_Rank() Over(order by Description,Markings)) as CatalogueNo
,Description
,Database_name
,Cable_Object_Identifier
From (
      Select Distinct
        C.Database_name
        ,C.Cable_Object_Identifier
        ,TRIM(Replace(Replace(
          Replace(Replace(Replace(CC.Description,'% GRAU MMA',''),' BLAU MMA',''),' BLAU',''),' GRAU',''),' ROT','')) 
          as Description
        ,concat_ws(',',
        to_get_sort(collect_list(C.Core_Markings) Over(partition by C.database_name,C.Cable_Object_Identifier))) as Markings
      From sigraph_silver.S_CableCoreCatalogue C
      inner join sigraph_silver.S_CableCatalogue CC
      on C.database_name == CC.database_name
      and C.Cable_object_identifier == CC.Object_identifier
      Where Cable_Object_Identifier is not null
) as A
--Select * from sigraph.R_Database_Cable_Catalogue_Number Where Cable_Object_Identifier is not null



