
Create Or Replace Temp View VW_EL_Pin
As
Select Distinct
 A.Database_Name
,A.Object_Identifier
,A.Dynamic_Class
,A.Function_Dynamic_Class
,A.Function_Object_Identifier
,A.Group_Overview
,A.Overview_Pin_Number
,A.Internal_Pin_Number
,Pin_Group
,Potential
,Pin_Designation
,Component_Function_Designation
,Generate_Pin
,Concat(Pin_Group,potential
      ,Case -- For terminal strip where we have different pin designation for same pin like pin=1 has ABCD, then apply below logic
            when IsTerminalStrip=1 and Pin_Side_Count>=4 and Pin_Component<>''
            Then Replace(TerminalSides,',',Pin_Component)
            -- For terminal strip where we have only 2 pin designation A,B for same pin like pin=1 has A&B, then apply below logic
            when IsTerminalStrip=1 and Pin_Side_Count<4 and Pin_Component<>''
            Then Pin_Component
            -- This condition is added for those devices like Gateway and others, where we dont have terminals defined.
            when IsPLC=0 and DeviceWithNoPin=1
            Then Generate_Pin     
			 else Coalesce(Pin_Designation,Pin_Component) 
        END)
         as Terminal_Marking 
from VW_EL_Pin_Prep_Query A      
Where 
-- Ignore Pin if They are PLC Module and has no pins. As these terminals marking will be loaded separately per Channel number
Case When IsPLC=1 and DeviceWithNoPin=1 then 0 else 1 end=1

