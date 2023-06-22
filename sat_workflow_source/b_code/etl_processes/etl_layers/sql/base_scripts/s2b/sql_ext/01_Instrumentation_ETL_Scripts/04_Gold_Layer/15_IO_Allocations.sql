

Select Distinct
Tag_Number
,Parent_Equipment_No
,IOType
,Equipment_No
,CatalogueNo
,ChannelNumber
From Sigraph_Silver.S_IO_Allocations
Where Class in ('Instrumentation','Inst(Shared)','Elec(Shared)')
and database_name in (Select Database_name from VW_Database_names)
