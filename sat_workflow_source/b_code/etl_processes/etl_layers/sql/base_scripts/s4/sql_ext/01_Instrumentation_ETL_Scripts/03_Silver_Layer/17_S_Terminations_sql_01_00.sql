
create or replace temp view Terminations
as
Select Distinct
 A.database_name
,A.object_identifier
,A.Cable as CableNumber
,Coalesce(Wire_Markings,'1') as Core_Markings
,Coalesce(A.From_Location,'') as Parent_Equipment_No
,Case When B.Type='Field Device' Then B.Tag_Number else Product_Key END as Equipment_No
,A.From_Terminal_Marking as Marking
,'R' as Left_Right
,DT.Class
From Sigraph_silver.S_Connection A
Inner join Sigraph_Silver.S_ItemFunction B On A.database_name=B.database_name and A.From_Dynamic_Class=B.Dynamic_Class
and A.From_Object_identifier=B.Object_identifier
Inner join Sigraph_Silver.S_Terminals DT On DT.Parent_Equipment_No=Coalesce(A.From_Location,'')
and DT.Equipment_No=Case When B.Type='Field Device' Then B.Tag_Number else Product_Key END
and DT.Marking=A.From_Terminal_Marking
WHERE  Coalesce(A.From_Location,'')<>Coalesce(A.TO_Location,'')

UNION
Select Distinct
 A.database_name
,A.object_identifier
,A.Cable as CableNumber
,Coalesce(Wire_Markings,'1')  as CoreMarkings
,Coalesce(A.To_Location,'') as Parent_Equipment_No
,Case When B.Type='Field Device' Then B.Tag_Number else B.Product_Key END as Equipment_No
,A.To_Terminal_Marking as Marking
,'L' as Left_Right
,DT.Class
From Sigraph_silver.S_Connection A
Inner join Sigraph_Silver.S_ItemFunction B On A.database_name=B.database_name and A.To_Dynamic_Class=B.Dynamic_Class
and A.To_Object_identifier=B.Object_identifier
Inner join Sigraph_Silver.S_Terminals DT 
On DT.Parent_Equipment_No=Coalesce(A.To_Location,'')
and DT.Equipment_No=Case When B.Type='Field Device' Then B.Tag_Number else B.Product_Key END
and DT.Marking=A.To_Terminal_Marking
WHERE Coalesce(A.From_Location,'')<>Coalesce(A.TO_Location,'')

order by 1,2

