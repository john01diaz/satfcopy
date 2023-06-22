
Select *
from sigraph_silver.S_Connection A
Left outer join sigraph_silver.S_Connection B ON A.database_name=B.database_name and A.To_Object_Identifier=B.From_Object_Identifier
Where A.FROM_object_identifier='ID_28_c_1f59338'

