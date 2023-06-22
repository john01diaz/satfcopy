
-- For IO we need to apply different logic, as it is not based on terminal markings but it is based on number of channels. Below logic takes the not null model no as is, wherever we have modelNo null , it generates the dummy moodelNo. If same model no has two different channel count, then generate that as new version.
Create Or Replace Temp View VW_Item_Function_Model_Extract_IO
As
   
Select Distinct
 A.database_name
,A.Dynamic_Class
,A.Object_Identifier
,A.Item_dynamic_class
,A.Item_Object_Identifier
,A.ModelNo    
,A.Symbol_Name 
,Case When A.Type<>'IO Module' Then Concat(A.ChannelNumber,'+') Else '' END  as Left_Marking
,Case When A.Type='IO Module' Then Concat(A.ChannelNumber,'+') Else '' END  as Right_Marking
from sigraph_silver.S_Itemfunction A
Where Type in ('IO Module','FTA') and A.ChannelNumber is not null and A.ChannelNumber<>'' and A.ChannelNumber<>'0'
and Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1

UNION
-- FTA need to add only +, as we need this to define the connection between IO and FTA.
Select Distinct
 A.database_name
,A.Dynamic_Class
,A.Object_Identifier
,A.Item_dynamic_class
,A.Item_Object_Identifier
,A.ModelNo    
,A.Symbol_Name 
,Case When A.Type<>'IO Module' Then Concat(A.ChannelNumber,'-') Else '' END  as Left_Marking
,Case When A.Type='IO Module' Then Concat(A.ChannelNumber,'-') Else '' END  as Right_Marking
from sigraph_silver.S_Itemfunction A
Where Type in ('IO Module') and A.ChannelNumber is not null and A.ChannelNumber<>'' and A.ChannelNumber<>'0'
and Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1
-- As per the IO rule, each IO should have two terminals.
-- If we have El PIn for PLC Function, but only one marking is there then add additional marking using below query



