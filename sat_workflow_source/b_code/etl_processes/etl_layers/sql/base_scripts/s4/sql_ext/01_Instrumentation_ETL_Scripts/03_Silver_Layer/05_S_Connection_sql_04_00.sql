
-- If for the same connection id, the pins are not categorized on left and right and we have special connections where it is connecting both to and from on same side then below logic will take care of this scenario.
CREATE OR REPLACE TEMP VIEW Vw_Connection_Pin_Extract_2
As
Select
A.object_identifier
,A.database_name
,A.From_Pin_Dynamic_Class
,A.From_Pin_Object_Identifier
,A.To_Pin_Dynamic_Class
,A.To_Pin_Object_Identifier
from VW_Connection_Pin_Unions A
Left Anti Join Vw_Connection_Pin_Extract_1 B On A.database_name=B.Database_name and A.Object_identifier=B.Object_Identifier


