
-- If all equipments are not able to extract usign Documents logic, use below logic, which will map the equipment to an instrument and get the Area and Areapath of an instrument and associate that to equipment.

Create OR REPLACE TEMP VIEW VW_EquipmentExtract_From_ItemFunction
AS
Select 
A.database_name
,A.Dynamic_Class
,A.Object_Identifier
,A.Area
,A.EquipmentNo
,A.AreaPath
From (
Select Distinct
l.Area
,A.Location_Designation as EquipmentNo
,l.AreaPath
,A.database_name
,A.Dynamic_Class
,A.Object_Identifier
,Row_Number() Over(Partition by A.Database_name,A.Location_Designation order by A.Loop_Number) as RNT
from sigraph_Silver.S_ItemFunction A
Inner join sigraph_Silver.S_Instrument_Index l 
on a.database_name=l.database_name 
and A.loop_element_dynamic_Class=l.Dynamic_Class 
and A.Loop_Element_Object_Identifier=l.Object_Identifier
Where A.Location_Designation is not null 
) as A 
-- Extract only those equipments, which are not present in document class
LEFT ANTI JOIN VW_EquipmentExtract_From_DocumentClasses B On A.Database_name=B.Database_name and A.EquipmentNo=B.EquipmentNo
Where A.RNT=1

