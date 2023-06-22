
Create or Replace Temp View VW_IO_Card_To_FTA_Connection
As 
Select * from VW_IO_Card_To_FTA_Connection_Query_1
UNION
Select * from VW_IO_Card_To_FTA_Connection_Query_2
UNION
Select * from VW_IO_Card_To_FTA_Connection_Query_3

