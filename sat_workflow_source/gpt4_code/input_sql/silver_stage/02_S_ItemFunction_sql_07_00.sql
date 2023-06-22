
CREATE OR REPLACE TEMP VIEW VW_ItemFunction
As
Select A.* 
from VW_ItemFunction_Prep_Query A
LEFT ANTI JOIN (
Select F.database_name,F.dynamic_class,F.object_identifier
from VW_ItemFunction_Prep_Query F
LEFT SEMI JOIN VW_ItemFunction_Prep_Query D ON F.database_name=D.database_name 
and F.Item_dynamic_class=D.Item_dynamic_class and F.Item_object_identifier=D.Item_object_identifier 
and D.type='Field Device' and F.Type='Device'
Where  F.database_name='R_2016R3' 
) as B ON A.database_name=B.database_name and A.dynamic_class=B.dynamic_class and A.object_identifier=B.object_identifier
where A.database_name == "R_2016R3" 

