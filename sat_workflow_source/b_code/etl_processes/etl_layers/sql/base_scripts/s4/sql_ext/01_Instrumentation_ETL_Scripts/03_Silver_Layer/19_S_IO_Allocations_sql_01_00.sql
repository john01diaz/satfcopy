
create or replace temp view IO_Allocations_Prep_Query
as
Select Distinct
 SR.database_name
,SR.Dynamic_Class
,SR.object_identifier
,SR.Item_Object_identifier
,SR.Item_Dynamic_Class 
,A.Loop_Number as Loop_Number
,SR.Location_Designation as Parent_Equipment_No
,SR.IOType
,SR.Product_Key as Equipment_No
,SR.ChannelNumber
,SR.Class
,SR.Document_Number
,J.From_Location as Junction_Box   
,A.To_dynamic_Class  
,A.To_object_Identifier
From (
Select Distinct
Coalesce(N.To_Item,M.To_Item,L.To_Item,K.To_Item,J.To_Item,I.To_Item,H.To_Item,G.To_Item,F.To_Item,E.To_Item
             ,D.To_Item,C.To_Item
             ,B.To_Item) as To_Item             
 ,Coalesce(N.To_dynamic_Class,M.To_dynamic_Class,L.To_dynamic_Class,K.To_dynamic_Class,J.To_dynamic_Class,I.To_dynamic_Class,H.To_dynamic_Class,G.To_dynamic_Class,F.To_dynamic_Class,E.To_dynamic_Class
             ,D.To_dynamic_Class,C.To_dynamic_Class
             ,B.To_dynamic_Class) as To_dynamic_Class    
,Coalesce(N.To_object_Identifier,M.To_object_Identifier,L.To_object_Identifier,K.To_object_Identifier,J.To_object_Identifier
,I.To_object_Identifier,H.To_object_Identifier,G.To_object_Identifier,
                            F.To_object_Identifier,E.To_object_Identifier,D.To_object_Identifier,C.To_object_Identifier
                            ,B.To_object_Identifier)as To_object_Identifier             
,A.Loop_Number
,A.Database_name
,A.Object_identifier
,A.From_Object_identifier
,A.From_Dynamic_Class      
,A.From_Item_Object_identifier
,A.From_Item_Dynamic_Class        
from sigraph_silver.S_Connection A
Inner join sigraph_silver.s_io_catalogue IOC ON IOC.database_name=A.database_name 
and IOC.object_identifier=A.From_Object_identifier
Left outer join sigraph_silver.S_Connection B ON A.database_name=B.database_name and A.To_Object_Identifier=B.From_Object_Identifier
Left outer join sigraph_silver.S_Connection C ON B.database_name=C.database_name and B.To_Object_Identifier=C.From_Object_Identifier
Left outer join sigraph_silver.S_Connection D ON C.database_name=D.database_name and C.To_Object_Identifier=D.From_Object_Identifier
Left outer join sigraph_silver.S_Connection E ON D.database_name=E.database_name and D.To_Object_Identifier=E.From_Object_Identifier
Left outer join sigraph_silver.S_Connection F ON E.database_name=F.database_name and E.To_Object_Identifier=F.From_Object_Identifier
Left outer join sigraph_silver.S_Connection G ON F.database_name=G.database_name and F.To_Object_Identifier=G.From_Object_Identifier
Left outer join sigraph_silver.S_Connection H ON G.database_name=H.database_name and G.To_Object_Identifier=H.From_Object_Identifier
Left outer join sigraph_silver.S_Connection I ON H.database_name=I.database_name and H.To_Object_Identifier=I.From_Object_Identifier
Left outer join sigraph_silver.S_Connection J ON I.database_name=J.database_name and I.To_Object_Identifier=J.From_Object_Identifier
Left outer join sigraph_silver.S_Connection K ON J.database_name=K.database_name and J.To_Object_Identifier=K.From_Object_Identifier
Left outer join sigraph_silver.S_Connection L ON K.database_name=L.database_name and K.To_Object_Identifier=L.From_Object_Identifier
Left outer join sigraph_silver.S_Connection M ON L.database_name=M.database_name and L.To_Object_Identifier=M.From_Object_Identifier
Left outer join sigraph_silver.S_Connection N ON M.database_name=N.database_name and M.To_Object_Identifier=N.From_Object_Identifier
           

) as A

-- Get the IO Card Details
Inner join Sigraph_silver.S_Itemfunction SR
ON Coalesce(item_slot,'')<>'' and Type='IO Module'
and SR.database_name=A.Database_name 
and SR.Loop_Number=A.Loop_Number
and SR.Dynamic_Class=A.From_Dynamic_Class
and SR.Object_identifier=A.From_Object_Identifier 
-- Get the junction box details for the device
Left outer join sigraph_silver.S_Connection J ON A.database_name=J.database_name 
and J.To_Object_Identifier =A.To_object_Identifier
and J.From_Dynamic_Class='LC_Component_function'  
Where Case When A.From_Dynamic_Class='LC_Component_function' and A.To_dynamic_Class='LC_Component_function' Then 0 
           When A.From_Dynamic_Class='LC_PLC_Function' and A.To_dynamic_Class='LC_PLC_Function' Then 0
           Else 1 End =1

