
-- -- Restricted with only limited properties, as per Zahra request on 12/12/2022
-- Select 
-- Area
-- ,AreaPath
-- ,ClassName
-- ,FormatName
-- ,TagNo
-- ,Function
-- ,Number
-- ,LoopFunc
-- ,suffix
-- ,LoopNo
-- ,LService1
-- ,ISALocation
-- ,Status
-- ,`Device_Type`
-- ,Instrument_Service
-- ,manufacturer_mce
-- ,ModelNo
-- ,ProcessEquipment
-- ,ProcessLines
-- ,Size
-- ,PIDNo
-- ,LoopDwgCode
-- ,DCSIO
-- ,OperatingPrinc
-- ,Remarks
-- ,`Wiring_Config`
-- ,`junction_box`
-- ,Case when `Wiring_Config`='' Then 'Mechanical Connection. Need to be added in the drawing' END as SpecialRemarks
-- from sigraph_silver.S_Instrument_Index