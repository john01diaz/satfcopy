
Select
  Distinct loop_element_cs_voltage_type as Voltage_Type,
  CS_voltage as Voltage
from
  Sigraph.CS_Layer_Loop_Loop_elements
Where
  loop_element_CS_voltage_type is not null
--   and loop_database_name in (select * from VW_Database_names)
  and loop_element_CS_voltage_type <> '_empty_'
  and loop_element_CS_voltage_type != " "
  and CS_voltage <> '+0'