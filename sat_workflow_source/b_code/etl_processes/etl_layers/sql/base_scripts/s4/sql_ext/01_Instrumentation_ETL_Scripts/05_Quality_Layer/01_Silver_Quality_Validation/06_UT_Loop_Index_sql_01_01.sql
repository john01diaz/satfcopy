

INSERT INTO SIGRAPH_SILVER.UNIT_TEST_RESULTS (Loader_Name,database_name,Object_Identifier,UT_ID,Test_Case)
SELECT "LOOP_INDEX" AS Loader_Name,
*
FROM UT_VW_LOOP_INDEX
