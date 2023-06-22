
Create Or Replace Temp View VW_Connection_Prep_Query
As
Select * from VW_Connection_Prep_Query_Non_IO Where From_dynamic_Class is not null and To_dynamic_Class is not null
UNION
Select * from VW_Connection_Prep_Query_IO     Where From_dynamic_Class is not null and To_dynamic_Class is not null

