
CREATE OR REPLACE TEMP VIEW VW_DeviceCatalogue_Extract
As

Select Distinct
"TRUE" as AllowUse
,VDV.Device_Type as Type
-- if we have ModelNo blank in Sigraph. add Product Key, Document number and database name in description to validate this details and update with correct data in future - 2/28/2023 Discipline.
,Coalesce(
            Concat(Coalesce(VDV.Description,'')
                  ,'-'
                  ,Coalesce(VDV.Product_Key,'')
                  ,'-'
                  ,Coalesce(VDV.Document_Number,'')
                  ,'-'
                  ,Replace(VDV.Database_name,'_2016R3','')
                  )
              ,'RHLDD') as Description	
,Manufacturer as Manufacturer	
,VM.ModelNo
,VDV.Class
,VM.Left
,VM.Right
,VM.Left_Marking
,VM.Right_Marking
,VM.Symbol_name
,VDV.Loop_Number
,VDV.Tag_Number
,VDV.Document_Number
,VDV.Product_Key as Product_Key
,VDV.database_name
,VDV.Item_Object_Identifier              
,VDV.Item_Dynamic_Class
,VDV.Object_Identifier
,VDV.Dynamic_Class
,Row_Number() Over(Partition by VM.ModelNo order by VDV.Item_Object_Identifier) as Catalogue_RNT
from sigraph_silver.S_Itemfunction VDV
Inner join sigraph_silver.S_Item_Function_Model VM 
On VDV.database_name=VM.database_name 
and VDV.Item_Dynamic_Class=VM.Item_Dynamic_Class
and VDV.Item_Object_identifier=VM.Item_Object_identifier
and VDV.Dynamic_Class=VM.Dynamic_Class
and VDV.Object_Identifier=VM.Object_Identifier
Where VDV.Type in ('Device','FTA') and VDV.location_designation is not null
and (Coalesce(VM.Left,0)+Coalesce(VM.Right,0))>0