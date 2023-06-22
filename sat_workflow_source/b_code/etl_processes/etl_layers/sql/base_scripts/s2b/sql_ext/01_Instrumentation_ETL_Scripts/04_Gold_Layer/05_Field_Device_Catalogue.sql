

Select Distinct 
  A.Catalogue_Name,
  A.Left_pin_details,
  A.Left_Marking,
  A.Right_pin_details,
  A.Right_Marking,
  A.Tag_Number,
  A.Loop_Number,
  A.Document_number
From SIGRAPH_SILVER.S_FIELD_DEVICE_CATALOGUE A
Where Catalogue_RNT=1 and Catalogue_Name<>''
and database_name in (Select Database_name from VW_Database_names)
