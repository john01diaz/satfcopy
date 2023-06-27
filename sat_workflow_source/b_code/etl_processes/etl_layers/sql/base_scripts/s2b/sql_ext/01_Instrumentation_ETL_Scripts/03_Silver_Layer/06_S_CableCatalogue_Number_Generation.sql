

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




 df = spark.sql('Select * from VW_CatalogueNo where database_name == "R_2016R3"')

 dbutils.fs.rm('dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_CableCatalogueNumber_Master',True)

 df = cleansing_df(df)

 df.write.save(
     format = 'delta'
    ,mode   = 'overwrite'
    ,path   = 'dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_CableCatalogueNumber_Master'
    ,overwriteSchema = True
 )
