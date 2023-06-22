
-- Get the Pin orientation where we have Symbol and Pin orientation defined
Create Or Replace Temp View VW_Item_Model_Prep_Query_2
As
Select Distinct
DP.database_name
,DP.Dynamic_Class
,DP.Object_Identifier
,DP.Item_Dynamic_Class
,DP.Item_Object_Identifier
,DP.ModelNo
,DP.Symbol_Name
,Case When Left_Marking is not null and 
      Dense_rank() Over(Partition by DP.database_name,DP.Item_Object_identifier,All_Marking order by DP.Object_identifier)>1
      Then Concat(
          Row_Number() Over(Partition by DP.database_name,DP.Item_Object_identifier,All_Marking order by DP.Object_identifier)
          ,'_'    
          ,Left_Marking
          )
      Else Coalesce(Left_Marking,'') END as Left_Marking
-- As terminal strips pin has only one pin pointed to Left and Right. We should not double count the same in both. So consider only on Left Side.
,Case When DP.item_dynamic_class='LC_Terminal_strip' Then ''
      When Right_Marking is not null and 
      Dense_rank() Over(Partition by DP.database_name,DP.Item_Object_identifier,All_Marking order by DP.Object_identifier)>1
      Then Concat(
          Row_Number() Over(Partition by DP.database_name,DP.Item_Object_identifier,All_Marking order by DP.Object_identifier)
          ,'_'    
          ,Right_Marking
          )
      Else Coalesce(Right_Marking,'') END as Right_Marking
From sigraph_silver.S_ItemFunction DP 
-- if The pins are not present in El PIN Class, Then consider the pin orientation using below symbol and pin cross walk.
LEFT ANTI JOIN VW_Item_Model_Prep_Query_1 ELP ON ELP.database_name=DP.database_name
and ELP.Item_Dynamic_Class=DP.Item_Dynamic_Class and ELP.Item_Object_Identifier=DP.Item_Object_Identifier

Inner join (
Select
Symbol_Name
,Coalesce(S.Top,S.Right) as Left_Marking
,Coalesce(S.Bottom,S.Left) as Right_Marking
,Coalesce(S.Top,S.Right,S.Unknown,S.Bottom,S.Left) as All_Marking
From sigraph_reference.Symbol_pin_orientation S
Where Coalesce(S.Top,S.Right,S.Bottom,S.Left) is not null
) as SPO ON SPO.Symbol_Name=Case When DP.Item_Dynamic_Class<>'LC_PLC_Module' Then DP.Symbol_Name END
Where Case When Type='IO Module' and (Coalesce(ChannelNumber,'')='' OR ChannelNumber='0') Then 0 Else 1 End=1


