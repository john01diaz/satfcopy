

Select Distinct
  regexp_extract(CS_loop_element_id, '[A-Za-z]+', 0) as Type,
  UPPER(TRIM(CS_device_type)) as InstrumentType
From Sigraph.CS_layer_loop_loop_elements
Where CS_location_full_designation is null 
and loop_element_database_name in (select * from VW_Database_names)
and  CS_device_type is not null 
and trim(CS_device_type) != ""
order by Type
