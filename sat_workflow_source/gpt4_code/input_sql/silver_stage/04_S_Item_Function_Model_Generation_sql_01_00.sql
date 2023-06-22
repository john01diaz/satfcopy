
-- Get the Pin orientation where we have Electrical Pins in El Pin Class
Create Or Replace Temp View VW_Item_Model_Prep_Query_1
As
Select Distinct
DP.database_name
,DP.Dynamic_Class
,DP.Object_Identifier
,DP.Item_Dynamic_Class
,DP.Item_Object_Identifier
,DP.ModelNo
,DP.Symbol_Name
,Case When DP.item_dynamic_class='LC_Terminal_strip' 
      Then LDP.Terminal_Side
      When LDP.Terminal_Side='Left' Then LDP.Terminal_Marking
      Else ''
      End as Left_Marking
-- As terminal strips pin has only one pin pointed to Left and Right. We should not double count the same in both. So consider only on Left Side.
,Case When DP.item_dynamic_class='LC_Terminal_strip' Then ''
      When LDP.Terminal_Side='Right' Then LDP.Terminal_Marking
      Else ''
      END as Right_Marking
From sigraph_silver.S_ItemFunction DP 
-- Left Marking
Inner join Sigraph_Silver.S_Pin LDP 
ON LDP.database_name=DP.database_name
and LDP.Function_Dynamic_Class=DP.Dynamic_Class
and LDP.Function_Object_Identifier=DP.Object_Identifier
and LDP.Pin_Type='EL_PIN'
-- Consider only those records, where we are getting El pin orientation
Where Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1


