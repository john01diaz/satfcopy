
-- Sigraph Cable
Create or Replace Temp View VW_CableSchedule
As
Select 
 Coalesce(A.database_name,C.Database_name) as database_name
,Coalesce(A.dynamic_class,C.dynamic_class) as dynamic_class
,Coalesce(A.object_identifier,C.object_identifier) as object_identifier
,Coalesce(A.CableNumber,C.CableNumber) as CableNumber
,Coalesce(A.From_Location,C.From_Location) as From_Location
,Coalesce(A.To_Location,C.To_Location) as To_Location
,Coalesce(A.CatalogueNo,C.CatalogueNo) as CatalogueNo
,Coalesce(A.Length,C.Length) as Length
,Coalesce(A.ProjectStatus,C.ProjectStatus) as ProjectStatus	
,A.Remarks	
,A.Gland_From	
,A.Gland_To	
,A.Adapter_From	
,A.Adapter_To	
,A.Discipline
-- Additonal Columns
,A.Range
,A.Cable_Drum_Number
,A.Laying_Corner_Point
,A.External_document_of_item
,A.Installed_Length
,A.Installation_Date
,A.Estimated_Length
,A.Function_text_1
,A.Level_of_Installation
,B.Shield_number
,B.Wire_number
,B.Cable_Set
,B.Wire_type
,B.Conductor_type
,B.Insulating_material
,B.Inductance_per_km
,B.Bending_radius
,B.Capacitance_per_km
,B.Shield
,B.Rated_voltage_Uo
,B.Rated_voltage_U
,B.Precious_metal_factor_2
,B.Precious_metal_factor_1
,B.Suppliers_article_no
,B.Mass
,B.Component_description_1
,B.Selection_key
,B.Outside_diameter
--,B.Article_number
,B.Subassembly_information
,B.Rated_temperature
,B.Quantity_unit
,B.Mounting_feature
,B.min_ambient_temperature
,B.Measure_unit_qualifier
,B.max_ambient_temperature
,B.List_price
,B.EAN_number
,B.Body_length
from VW_Cable_Extract A
Left outer join VW_Comp_Additional_property B On A.CatalogueNo=B.CatalogueNo
-- Fabricated cable
Full Outer join VW_Fabricated_Cable_Extract C ON A.CableNumber=C.CableNumber


