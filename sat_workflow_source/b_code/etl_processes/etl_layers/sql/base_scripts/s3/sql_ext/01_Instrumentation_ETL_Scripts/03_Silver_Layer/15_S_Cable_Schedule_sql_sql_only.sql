
-- Extract the cable schedule using connection class, where we have cable details
-- The connections where we  have cables will be extracted in next view
CREATE OR REPLACE TEMP VIEW VW_Cable_Extract
AS
Select Distinct
 C.Database_name
,C.dynamic_class
,C.Object_Identifier
,C.LC_item_product_des as CableNumber
,Con.From_Location
,Con.To_Location
,CLM.CatalogueNo as CatalogueNo
,Cast(LC_cable_length as Decimal(18,2)) as Length
,Case When UPPER(CS_loop_status) like '%PLAN%' Then 'Being Planned' 
      When UPPER(CS_loop_status) like '%INA%' Then 'Remove'
      Else 'As Built'
      END as ProjectStatus
,CS_loop_status_remark as Remarks
,'' as Gland_From
,'' as Gland_To
,'' as Adapter_From
,'' as Adapter_To	
,CL.Class as Discipline
-- Additional properties
,CL.size as Cross_section
,CL.Class

,C.LC_cable_drum_number As Cable_Drum_Number
,CL.Colour1  As Color
,C.LC_installation_vertices As Laying_Corner_Point
,C.LC_Item_ext_document As External_document_of_item
,C.LC_installed_cable_length As Installed_Length
,C.LC_installation_date As Installation_Date
,C.LC_cable_length As Estimated_Length
,C.LC_item_function_text_1 As Function_text_1
,C.LC_installation_level As Level_of_Installation
-- Picklist Property
,get_loop_picklist_display_value(array('LC_range',C.LC_range)) as `Range`
from sigraph.Cable C 
Inner join sigraph_Silver.S_CableCatalogue as CL ON C.database_name=CL.database_name and C.Object_Identifier=CL.Object_Identifier
Inner join sigraph_Silver.S_CableCatalogueNumber_Master as CLM ON C.database_name=CLM.database_name and C.Object_Identifier=CLM.Cable_Object_Identifier
Left outer join sigraph_Silver.S_Connection Con ON Con.database_name=C.database_name and Con.Cable=C.LC_item_product_des
-- Loop Mapping
Left outer join (
Select Distinct 
Con.database_name
,Con.Cable_Object_Identifier
,l.CS_loop_status
,l.CS_loop_status_remark
From (
      Select * From sigraph_Silver.S_Connection 
      qualify Row_Number() Over(Partition by database_name,Cable_Object_Identifier order by object_identifier)=1
     )Con
Inner join sigraph.loop l 
on l.loop_database_name=Con.database_name 
and l.CS_loop_id=Con.Loop_Number
) as LP 
ON LP.database_name=C.database_name 
and LP.Cable_Object_Identifier=C.object_identifier;

-- cabinet to cabinet where we dont have cable connection will be extracted using below script.
CREATE OR REPLACE TEMP VIEW VW_Fabricated_Cable_Extract
As
Select Distinct
 Con.Database_name
,'LC_Connection' as Dynamic_Class
,Con.Object_Identifier
,Concat(Replace(Con.database_name,'_2016R3',''),'_',Con.Object_Identifier) as CableNumber
,From_Location as From_Location
,To_Location as To_Location
,'SHRH_CABLE_0000' as CatalogueNo
,0 as Length
,'As Built' as ProjectStatus	
,'' as Remarks	
,'' as Gland_From	
,'' as Gland_To	
,'' as Adapter_From	
,'' as Adapter_To	
,'Inst(Shared)' as Discipline
From sigraph_Silver.S_Connection Con
Where From_Location<>To_Location and Cable_Object_Identifier is null;

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
QUALIFY Row_Number() Over(Partition by CL.CatalogueNo order by A.Object_identifier) = 1;

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