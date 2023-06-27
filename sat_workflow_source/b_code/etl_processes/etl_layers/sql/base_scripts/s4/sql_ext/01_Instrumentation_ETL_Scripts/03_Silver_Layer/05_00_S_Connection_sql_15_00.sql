
Create Or Replace TEMP VIEW VW_Connection_Final_Extract
AS
-- Logic to update correct to and from connections based on Y coordinates of a symbol
SELECT Distinct
Con.object_identifier
,con.database_name
,Con.From_connection_pin_href
,Con.To_connection_pin_href

,Case When From_Geo_Point_Y<=To_Geo_Point_Y Then Con.To_dynamic_Class Else Con.FROM_dynamic_Class End as FROM_dynamic_Class
,Case When From_Geo_Point_Y<=To_Geo_Point_Y Then Con.To_object_identifier Else Con.FROM_object_identifier End as FROM_object_identifier
,Case When From_Geo_Point_Y<=To_Geo_Point_Y Then Con.To_Item_dynamic_Class Else Con.FROM_Item_dynamic_Class End as FROM_Item_dynamic_Class
,Case When From_Geo_Point_Y<=To_Geo_Point_Y Then Con.To_Item_object_identifier Else Con.FROM_Item_object_identifier End as FROM_Item_object_identifier

,Case When From_Geo_Point_Y<=To_Geo_Point_Y Then Con.From_dynamic_class Else Con.To_dynamic_Class End as To_dynamic_Class
,Case When From_Geo_Point_Y<=To_Geo_Point_Y Then Con.FROM_object_identifier Else Con.To_object_identifier End as To_object_identifier
,Case When From_Geo_Point_Y<=To_Geo_Point_Y Then Con.FROM_Item_dynamic_Class Else Con.To_Item_dynamic_Class End as To_Item_dynamic_Class
,Case When From_Geo_Point_Y<=To_Geo_Point_Y Then Con.FROM_Item_object_identifier Else Con.To_Item_object_identifier End as To_Item_object_identifier

,Con.Cable_Object_Identifier
,Con.Wire_Object_Identifier

,Case When From_Geo_Point_Y<=To_Geo_Point_Y Then Con.To_Location Else Con.From_Location End as From_Location
,Case When From_Geo_Point_Y<=To_Geo_Point_Y Then Con.To_Facility Else Con.From_Facility End as From_Facility
,Case When From_Geo_Point_Y<=To_Geo_Point_Y Then Con.To_Item Else Con.From_Item End as From_Item
,Case When From_Geo_Point_Y<=To_Geo_Point_Y Then Con.To_Terminal_Marking Else Con.From_Terminal_Marking End as From_Terminal_Marking
,Cable
,Wire_Cross_Section
,Wire_Markings

,Case When From_Geo_Point_Y<=To_Geo_Point_Y Then Con.From_Location Else Con.To_Location End as To_Location
,Case When From_Geo_Point_Y<=To_Geo_Point_Y Then Con.From_Facility Else Con.To_Facility End as To_Facility
,Case When From_Geo_Point_Y<=To_Geo_Point_Y Then Con.From_Item Else Con.To_Item End as To_Item
,Case When From_Geo_Point_Y<=To_Geo_Point_Y Then Con.From_Terminal_Marking Else Con.To_Terminal_Marking End as To_Terminal_Marking

,Con.Loop_Number
,Con.Tag_Number
,Con.Document_Number
,Connection_Type
,From_Geo_Point_Y
,To_Geo_Point_Y
From (
SELECT 
Con.*
,Coalesce(D.From_Geo_Point_Y,0) as From_Geo_Point_Y
,Coalesce(E.To_Geo_Point_Y,0) as To_Geo_Point_Y
FROM VW_Connection_PreFinal_Extract Con
Inner join sigraph_silver.S_Itemfunction B ON Con.database_name=B.databasE_name 
and Con.FROM_object_identifier=B.object_identifier
Inner join sigraph_silver.S_Itemfunction C ON Con.database_name=C.databasE_name 
and Con.To_object_identifier=C.object_identifier
left outer join 
(
Select *
,Cast(Geo_Point_Y as Decimal(18,2)) as From_Geo_Point_Y
From sigraph.Function_occ
)as   D On B.database_name=D.database_name and B.Function_Occ_Object_identifier=D.object_identifier
left outer join (
Select *
,Cast(Geo_Point_Y as Decimal(18,2)) as To_Geo_Point_Y
From sigraph.Function_occ
)as  E On C.database_name=E.database_name and C.Function_Occ_Object_identifier=E.object_identifier
) as Con

