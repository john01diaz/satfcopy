
create or replace temp view Internal_Wiring
as
Select Distinct
 A.database_name
,A.object_identifier 
,A.From_Location as From_Parent_Equipment_No
,'' as From_Compartment
,A.From_Item as From_Equipment
,A.Connection_Type as From_Wire_Link
,A.From_Terminal_Marking as From_Marking
-- If the connection is from Terminal strip and if we are unable to find the side of terminal strip (Which can be decided based on Pin Designation (A is left and B is right)). Then put them under Normal connection
,Case when FROM_dynamic_Class='LC_Component_function' 
      and UPPER(regexp_extract(A.From_Terminal_Marking,'[A-Za-z]+',0)) not in ('AB','CD','EF','GH')
      Then 'Normal'
      Else 'R'
      END as From_Left_Right
,A.To_Location as To_Parent_Equipment_No
,'' as To_Compartment
,A.To_Item as To_Equipment_No
,A.Connection_Type as To_Wire_Link
,A.To_Terminal_Marking as To_Marking
,Case when To_dynamic_Class='LC_Component_function' 
      and UPPER(regexp_extract(A.To_Terminal_Marking,'[A-Za-z]+',0)) not in ('AB','CD','EF','GH')
      Then 'Normal'
      Else 'L'
      END as To_Left_Right 
,DF.Class      
from Sigraph_silver.S_Connection A
Inner join Sigraph_Silver.S_Terminals DF 
On DF.Parent_Equipment_No=Coalesce(A.From_Location,'')
and DF.Equipment_No=A.From_Item 
and DF.Marking=A.From_Terminal_Marking
Inner join Sigraph_Silver.S_Terminals DT 
On DT.Parent_Equipment_No=Coalesce(A.To_Location,'')
and DT.Equipment_No=A.To_Item 
and DT.Marking=A.To_Terminal_Marking
Where A.From_Location=A.To_Location -- Internal wiring logic, both to and from equipment should be same.
and A.From_Location  is not null  and A.To_Location is not null 

