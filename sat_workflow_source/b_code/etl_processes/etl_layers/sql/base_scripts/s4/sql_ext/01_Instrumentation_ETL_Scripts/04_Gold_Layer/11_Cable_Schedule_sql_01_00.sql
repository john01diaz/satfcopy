
Select Distinct
 A.CableNumber
,A.From_Location
,A.To_Location
,A.CatalogueNo
,A.Length
,A.ProjectStatus	
,A.Remarks	
,A.Gland_From	
,A.Gland_To	
,A.Adapter_From	
,A.Adapter_To	
,A.Range
,A.Cable_Drum_Number as `Cable Drum Number`
,A.Laying_Corner_Point as `Laying Corner Point`
,A.External_document_of_item as `External document of item`
,A.Installed_Length as `Installed Length`
,A.Installation_Date as `Installation Date`
,A.Estimated_Length As `Estimated Length`
,A.Function_text_1 As `Function text 1`
,A.Level_of_Installation As `Level of Installation`
,A.Shield_number As `Shield number`
,A.Wire_number As `Wire number`
,A.Cable_Set As `Cable Set`
,A.Wire_type As `Wire type`
,A.Conductor_type As `Conductor type`
,A.Insulating_material As `Insulating material`
,A.Inductance_per_km As `Inductance Per km`
,A.Bending_radius As `Bending radius`
,A.Capacitance_per_km As `Capacitance Per km`
,A.Shield As `Shield`
,A.Rated_voltage_Uo As `Rated_voltage Uo`
,A.Rated_voltage_U As `Rated_voltage U`
,A.Precious_metal_factor_2 As `Precious metal factor 2`
,A.Precious_metal_factor_1 As `Precious metal factor 1`
,A.Suppliers_article_no As `Suppliers article number`
,A.Mass As `Mass`
,A.Component_description_1 As `Component description 1`
,A.Selection_key As `Selection key`
,A.Outside_diameter As `Outside diameter`
,A.Subassembly_information As `Subassembly information`
,A.Rated_temperature As `Rated temperature`
,A.Quantity_unit As `Quantity unit`
,A.Mounting_feature As `Mounting feature`
,A.min_ambient_temperature As `minimum ambient temperature`
,A.Measure_unit_qualifier As `Measure unit qualifier`
,A.max_ambient_temperature As `maximum ambient temperature`
,A.List_price As `List price`
,A.EAN_number As `EAN number`
,A.Body_length As `Body length`
from Sigraph_Silver.S_CableSchedule A
Where database_name in (Select Database_name from VW_Database_names)
