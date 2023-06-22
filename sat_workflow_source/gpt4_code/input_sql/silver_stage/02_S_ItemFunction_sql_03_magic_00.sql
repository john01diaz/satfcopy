

-- Item Classes union- Get the required columns from each Item class
Create Or Replace Temp View VW_ItemClass
As
Select
 B.object_identifier
,B.database_name
,B.dynamic_class
,B.LC_item_complete_part_string
,B.LC_item_product_des
,B.LC_location_key_dyn_class
,B.LC_location_key_href
,B.LC_item_fac_rel_dyn_class
,B.LC_item_fac_rel_href
,B.LC_item_parent_product_key_dyn_class
,B.LC_item_parent_product_key_href
,'' as LC_plc_item_slot
,K.LC_product_key
,K.LC_show_key
,'Device' as Type
From sigraph.General_Item B
Inner join sigraph.Item_K K ON K.database_name=B.database_name and K.LC_Item_k_Rel_dyn_class=B.dynamic_Class and K.LC_Item_k_Rel_href=B.object_identifier
UNION
-- MAGIC
Select
 B.object_identifier
,B.database_name
,B.dynamic_class
,B.LC_item_complete_part_string
,B.LC_item_product_des
,B.LC_location_key_dyn_class
,B.LC_location_key_href
,B.LC_item_fac_rel_dyn_class
,B.LC_item_fac_rel_href
,B.LC_item_parent_product_key_dyn_class
,B.LC_item_parent_product_key_href
,B.LC_plc_item_slot
,K.LC_product_key
,K.LC_show_key
,Case when Coalesce(A.LC_plc_function_channel,'')<>'' --and Coalesce(A.LC_plc_function_channel,'')<>'0' 
      and PS.object_identifier is not null then 'IO Module'
      When Coalesce(A.LC_plc_function_channel,'')<>'' -- and Coalesce(A.LC_plc_function_channel,'')<>'0' 
      and B.LC_Item_Pin_group_Rel is null then 'IO Module'
      ELSE 'FTA'
      END as Type
from  sigraph.PLC_Module B 
Inner join sigraph.Item_K K ON K.database_name=B.database_name and K.LC_Item_k_Rel_dyn_class=B.dynamic_Class and K.LC_Item_k_Rel_href=B.object_identifier
left outer join sigraph.PLC_Function_special PS ON PS.database_name=B.databasE_name and PS.LC_PLC_Module_PLC_Function_group_href=B.object_identifier 
left outer join sigraph.PLC_Function A ON A.database_name=B.database_name and A.LC_item_functions_rel_href=B.object_identifier
UNION
-- MAGIC
Select
 B.object_identifier
,B.database_name
,B.dynamic_class
,A.LC_item_function_complete_part_name as LC_item_complete_part_string
,B.LC_item_product_des
,B.LC_location_key_dyn_class
,B.LC_location_key_href
,B.LC_item_fac_rel_dyn_class
,B.LC_item_fac_rel_href
,B.LC_item_parent_product_key_dyn_class
,B.LC_item_parent_product_key_href
,'' as LC_plc_item_slot
,K.LC_product_key
,K.LC_show_key
,'Terminal Strip' as Type
From sigraph.Terminal_Strip B
Inner join sigraph.Item_K K ON K.database_name=B.database_name and K.LC_Item_k_Rel_dyn_class=B.dynamic_Class and K.LC_Item_k_Rel_href=B.object_identifier
Left outer join sigraph.Component_function A ON A.database_name=B.database_name 
and A.LC_item_functions_rel_href=B.object_identifier
-- MAGIC
UNION
-- MAGIC
Select
 B.object_identifier
,B.database_name
,B.dynamic_class
,B.LC_item_complete_part_string
,B.LC_item_product_des
,B.LC_location_key_dyn_class
,B.LC_location_key_href
,B.LC_item_fac_rel_dyn_class
,B.LC_item_fac_rel_href
,B.LC_item_parent_product_key_dyn_class
,B.LC_item_parent_product_key_href
,'' as LC_plc_item_slot
,K.LC_product_key
,K.LC_show_key
,'Device' as Type
From sigraph.Integrated_Item B
Inner join sigraph.Item_K K ON K.database_name=B.database_name and K.LC_Item_k_Rel_dyn_class=B.dynamic_Class and K.LC_Item_k_Rel_href=B.object_identifier

