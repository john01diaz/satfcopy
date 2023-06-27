
-- For same model and channel if for one IO card, we have 2 terminals and for few others we have only 1 terminal, then mark the 2 terminal marking from the IO Cards, where we already have 2 terminals.

CREATE OR REPLACE TEMP VIEW VW_IO_TerminalMarking_Prep
As
Select
A.database_name
    ,A.dynamic_class
    ,A.object_identifier
    ,A.item_dynamic_class
    ,A.item_object_identifier
    ,A.IOType
    ,A.ChannelNumber
    ,A.Model_Number
    ,A.TerminalsPerMarking
    ,A.Terminal_Count
from VW_IO_TerminalMarking A
Where Terminal_Count>=2

UNION

SELECT
A.database_name
    ,A.dynamic_class
    ,A.object_identifier
    ,A.item_dynamic_class
    ,A.item_object_identifier
    ,A.IOType
    ,A.ChannelNumber
    ,A.Model_Number
    ,Coalesce(B.TerminalsPerMarking,A.TerminalsPerMarking) as TerminalsPerMarking
    ,Coalesce(B.Terminal_Count,A.Terminal_Count) as Terminal_Count
From VW_IO_TerminalMarking A
Left outer join VW_IO_TerminalMarking B On A.database_name=B.database_name and A.Model_Number=B.model_number 
and A.ChannelNumber=B.ChannelNumber and B.Terminal_Count=2
Where A.Terminal_Count=1

