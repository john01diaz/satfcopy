DROP TABLE sigraph_SILVER.S_Item_function;
CREATE TABLE sigraph_SILVER.S_Item_function
USING DELTA
LOCATION "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Item_function"


DROP TABLE sigraph_SILVER.S_PIN;
CREATE TABLE sigraph_SILVER.S_PIN
USING DELTA
LOCATION "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Pin"


DROP TABLE sigraph_SILVER.S_Connection;
CREATE TABLE sigraph_SILVER.S_Connection
USING DELTA
LOCATION "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Connection"


DROP TABLE sigraph_SILVER.S_CableCatalogue;
CREATE TABLE sigraph_SILVER.S_CableCatalogue
USING DELTA
LOCATION "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_CableCatalogue"


DROP TABLE sigraph_SILVER.S_CableCoreCatalogue;
CREATE TABLE sigraph_SILVER.S_CableCoreCatalogue
USING DELTA
LOCATION "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_CableCoreCatalogue"


DROP TABLE sigraph_SILVER.S_Device_Catalogue;
CREATE TABLE sigraph_SILVER.S_Device_Catalogue
USING DELTA
LOCATION "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Device_Catalogue"


DROP TABLE sigraph_SILVER.S_Field_Device_Catalogue;
CREATE TABLE sigraph_SILVER.S_Field_Device_Catalogue
USING DELTA
LOCATION "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Field_Device_Catalogue"


CREATE TABLE SIGRAPH_SILVER.S_IO_CATALOGUE
USING DELTA
LOCATION "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_IO_Catalogue"


DROP TABLE  SIGRAPH_SILVER.S_Loop_Index;
create table sigraph_silver.S_Loop_Index
using delta
location "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Loop_Index"


DROP TABLE  SIGRAPH_SILVER.S_Instrument_Index;
CREATE TABLE SIGRAPH_SILVER.S_Instrument_Index
USING DELTA 
LOCATION "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Instrument_Index";


DROP TABLE if exists  sigraph_silver.s_major_equipments;
CREATE TABLE SIGRAPH_SILVER.S_MAJOR_EQUIPMENTS
USING DELTA
LOCATION "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Major_Equipments"


DROP TABLE SIGRAPH_SILVER.S_Component;
CREATE TABLE SIGRAPH_SILVER.S_Component
USING DELTA
LOCATION "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Component"


DROP TABLE IF EXISTS SIGRAPH_SILVER.S_CableSchedule;
CREATE TABLE SIGRAPH_SILVER.S_CableSchedule
USING DELTA
LOCATION "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_CableSchedule";


DROP TABLE IF EXISTS SIGRAPH_SILVER.S_Terminals;
CREATE TABLE SIGRAPH_SILVER.S_Terminals
USING DELTA
LOCATION "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Terminals"


CREATE TABLE Sigraph_Silver.S_Terminations
USING DELTA
LOCATION "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Terminations"


DROP TABLE IF EXISTS sigraph_silver.S_Internal_Wiring;
CREATE TABLE sigraph_silver.S_Internal_Wiring
USING DELTA
LOCATION "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_Internal_Wiring"


CREATE TABLE sigraph_silver.S_IO_Allocations
USING DELTA
LOCATION "dbfs:/mnt/bclearer/temp/anusha_folder/sigraph_silver/S_IO_Allocations"
