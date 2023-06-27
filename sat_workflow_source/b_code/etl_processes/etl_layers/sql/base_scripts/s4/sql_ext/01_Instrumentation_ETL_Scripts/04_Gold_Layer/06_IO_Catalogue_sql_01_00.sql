
SELECT 
Model_Number
,Description
,Manufacturer
,DescriptionDrawing
,Channel
,AllowUse
,IOType
,NoOfPoints
,TerminalsPerPointChannel
,TerminalsPerMarking
,ChannelNumber
FROM SIGRAPH_SILVER.S_IO_CATALOGUE
Where Catalogue_RNT=1 
and database_name in (Select Database_name from VW_Database_names)
order by Model_Number,IOType,Cast(Translate(ChannelNumber,'AI_,AO_,DI_,DO_,FIELD BUS_','') as Bigint),TerminalsPerMarking
