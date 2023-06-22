
-- Get all the Major Equipments from Main_Location and Sub_Location

CREATE OR REPLACE TEMP VIEW VW_Location
AS
Select 
*
,Row_Number() Over(Partition by Database_name,EquipmentNo order by L.object_identifier) as RNT
From (
Select 
S.database_name
,S.dynamic_class
,S.object_identifier
,S.LC_location_key as EquipmentNo
,Null As Type
,Null As Designation
,Null As Comment
,Null As Installation_site
,Null As Category
from sigraph.Sub_location S
UNION
Select 
S.database_name
,S.dynamic_class
,S.object_identifier
,S.LC_location_key as EquipmentNo
,S.LC_main_location_type As Type
,S.LC_location_description As Designation
,S.LC_location_comment As Comment
,S.LC_main_location_site_dummy As Installation_site
,S.LC_main_location_type_dummy As Category
from sigraph.Main_location S
) as L 

