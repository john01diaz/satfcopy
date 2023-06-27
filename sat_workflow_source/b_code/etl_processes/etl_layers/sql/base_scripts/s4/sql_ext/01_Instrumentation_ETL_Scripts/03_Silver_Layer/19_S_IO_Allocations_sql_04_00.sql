
create or replace temp view IO_Allocations
as
Select Distinct
 SR.database_name
,SR.Dynamic_Class
,SR.object_identifier
,SR.Item_Object_identifier
,SR.Item_Dynamic_Class 
,SR.Tag_Number
,SR.Parent_Equipment_No
,SR.IOType
,SR.Equipment_No
,IOC.Model_Number as CatalogueNo
,SR.ChannelNumber
,SR.Class
,SR.Junction_Box
,0 as IsSoftTag
,SR.Document_Number
,SR.Loop_Number 
from IO_Allocations_Instrument_Mapping SR
Inner join sigraph_silver.s_io_catalogue IOC ON IOC.database_name=SR.database_name 
and IOC.object_identifier=SR.Object_identifier

union

Select Distinct
 SR.database_name
,SR.Dynamic_Class
,SR.object_identifier
,SR.Item_Object_identifier
,SR.Item_Dynamic_Class 
,SR.Tag_Number
,SR.Parent_Equipment_No
,SR.IOType
,SR.Equipment_No
,IOC.Model_Number as CatalogueNo
,SR.ChannelNumber
,SR.Class
,SR.Junction_Box
,1 as IsSoftTag
,SR.Document_Number
,SR.Loop_Number 
from IO_Allocations_Device_Mapping SR
Inner join sigraph_silver.s_io_catalogue IOC ON IOC.database_name=SR.database_name 
and IOC.object_identifier=SR.Object_identifier

