
-- Additional Columns Extracts, map this to the respective view and add it in the final extract
Create OR Replace TEMP View VW_Comp_Additional_property
As
Select 
A.database_name
,A.dynamic_class
,A.object_identifier
,CL.CatalogueNo
,comp_shield_no As Shield_number
,comp_number_code As Wire_number
,comp_bundle_no As Cable_Set
,comp_inductance_per_km As Inductance_per_km
,comp_bending_radius As Bending_radius
,comp_capacitance_per_km As Capacitance_per_km
,comp_rated_voltage_uo As Rated_voltage_Uo
,comp_rated_voltage_u As Rated_voltage_U
,comp_metal_factor_2 As Precious_metal_factor_2
,comp_metal_factor_1 As Precious_metal_factor_1
,comp_suppliers_article_no As Suppliers_article_no
,comp_mass As Mass
,comp_description_1 As Component_description_1
,comp_selection_key As Selection_key
,comp_outside_diameter As Outside_diameter
,comp_rated_temperature As Rated_temperature
,comp_min_ambient_temp As min_ambient_temperature
,comp_max_ambient_temp As max_ambient_temperature
,comp_list_price As List_price
,comp_ean_number As EAN_number
,comp_body_length As Body_length
-- Picklist Property
,get_loop_picklist_display_value(array('comp_insulating_material',Replace(comp_insulating_material,'PE (Poly-Ethylen)','PE (poly-ethylene)'))) As Insulating_material
,get_loop_picklist_display_value(array('comp_conductor_application',comp_conductor_application)) as `Conductor_type`
,get_loop_picklist_display_value(array('comp_core_type',comp_core_type)) as Wire_type
,get_loop_picklist_display_value(array('comp_measure_unit_qualifier',comp_measure_unit_qualifier)) as `Measure_unit_qualifier`
,get_loop_picklist_display_value(array('comp_mounting_feature',comp_mounting_feature)) as `Mounting_feature`
,get_loop_picklist_display_value(array('comp_quantity_unit',comp_quantity_unit)) as `Quantity_unit`
,get_loop_picklist_display_value(array('comp_shield',comp_shield)) as Shield
,get_loop_picklist_display_value(array('comp_subassembly_info',comp_subassembly_info)) as `Subassembly_information`
from sigraph.Component A
Inner join sigraph.Comp_data_kw_data B on A.database_name=B.database_name and A.object_identifier=B.channel_list_href
Inner join sigraph.Comp_kind_w_data C on A.database_name=C.database_name and A.kind_data_href=C.object_identifier
Inner join sigraph.Comp_data_kw_core D ON B.database_name=D.database_name and B.object_identifier=D.kw_core_list_href
Inner join sigraph_Silver.S_CableCatalogueNumber_Master as CL ON 
CL.Description=Replace(Replace(Replace(Replace(Replace(UPPER(A.Comp_Name),'NEU',' NS'),'~',''),'_',''),'ALT',''),'?M','µm')
-- where A.database_name  in (Select Database_name from VW_Database_names)
-- Do not add database name filter, as we might miss the cable description which is present in other databases in component class and same cable present in R database.
QUALIFY Row_Number() Over(Partition by CL.CatalogueNo order by A.Object_identifier) = 1

