
Create Or Replace Temp View Vw_Connection_Pin_Extract_1
As
Select * 
from (
Select
Con.object_identifier
,Con.database_name
-- Max is used as for same connection id we will get from and two connection in two different rows in above command. So to get the From Pin and To Pin for same connection id, grouped the data set based on connection id and considered max of pins.
,MAX(RPN.Dynamic_Class) as From_Pin_Dynamic_Class
,MAX(RPN.Object_Identifier) as From_Pin_Object_Identifier
,MAX(LPN.Dynamic_Class) as To_Pin_Dynamic_Class
,MAX(LPN.Object_Identifier) as To_Pin_Object_Identifier
From (
Select 
object_identifier
,database_name
,From_Pin_Object_Identifier as Pin_Object_Identifier
,From_Pin_Dynamic_Class as Pin_Dynamic_Class
From VW_Connection_Pin_Unions
UNION
Select 
object_identifier
,database_name
,To_Pin_Object_Identifier as Pin_Object_Identifier
,To_Pin_Dynamic_Class as Pin_Dynamic_Class
From VW_Connection_Pin_Unions
) as Con
--Right Pin
Left outer join sigraph_silver.S_Pin RPN ON RPN.database_name=Con.database_name 
and RPN.Object_Identifier=Con.Pin_Object_Identifier and RPN.Dynamic_Class=Con.Pin_Dynamic_Class
and Coalesce(RPN.Terminal_Side,'Left')='Right'
--Left Pin
Left outer join sigraph_silver.S_Pin LPN ON LPN.database_name=Con.database_name 
and LPN.Object_Identifier=Con.Pin_Object_Identifier and LPN.Dynamic_Class=Con.Pin_Dynamic_Class
and LPN.Terminal_Side='Left'
Where Coalesce(RPN.Object_Identifier,LPN.Object_Identifier) is not null
Group by Con.object_identifier,Con.database_name
) as A Where  From_Pin_Object_Identifier is not null and To_Pin_Object_Identifier  is not null

