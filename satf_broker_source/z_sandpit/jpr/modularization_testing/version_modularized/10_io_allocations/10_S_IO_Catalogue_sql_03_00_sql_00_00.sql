CREATE OR REPLACE TEMP VIEW VW_IO_CATALOGUE
AS
Select
A.database_name
,A.dynamic_class
,A.object_identifier
,A.Model_Number
,A.Description
,A.Manufacturer
,'' as DescriptionDrawing
,'' as Channel
,'True' as AllowUse	
,A.IOType
,A.NoOfPoints
,2 as TerminalsPerPointChannel	
,B.TerminalsPerMarking
,A.Class
,Row_Number() Over(Partition by A.Model_Number,A.NoOfPoints,B.TerminalsPerMarking 
              order by A.object_identifier) as Catalogue_RNT
From VW_IOCard_Prep_Query A
Inner join VW_IO_TerminalMarking B ON A.database_name=B.database_name and A.dynamic_Class=B.dynamic_Class
and A.Object_identifier=B.Object_identifier
Order by A.Model_Number,A.NoOfPoints,Cast(A.ChannelNumber as Bigint)
