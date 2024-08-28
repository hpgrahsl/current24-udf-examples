-------------------------------
-- sample queries for HELLO_UDF
-------------------------------

-- valid calls
SELECT who,lang,
    HELLO_UDF(who,lang) as udf_output
FROM hello;

-- no calls on taskmanager for the UDF 
SELECT id,who,lang,
    HELLO_UDF('Austin','EN') as udf_output
FROM hello;

------------------------------
-- sample queries for CALC_UDF
------------------------------

-- valid calls
SELECT id,operation,operand1,operand2,
    CALC_UDF(ROW(operation,operand1,operand2)) as udf_output
FROM calc;

SELECT operation,operand1,operand2,
    CALC_UDF(ROW('ADD',18,9)) as udf_output
FROM calc;

------------------------------------
-- sample queries for OVERLOADED_UDF
------------------------------------

-- valid calls
-- 4 rows, 2 UDF calls without constants -> 8 calls on taskmanager 
SELECT id,
    OVERLOADED_UDF(data1) as udf_output1,
    OVERLOADED_UDF(data2,data2) as udf_output2
FROM overloaded;

-- 4 rows, 1 UDF call with/1 UDF call without constants -> 4 calls on taskmanager
SELECT id,
    OVERLOADED_UDF(data1) as udf_output1,
    OVERLOADED_UDF(12.34,43.21) as udf_output2
FROM overloaded;

-- 4 rows, 2 UDF calls with constants -> 0 calls on taskmanager
SELECT id,
    OVERLOADED_UDF('foo') as udf_output1,
    OVERLOADED_UDF(12.34,43.21) as udf_output2
FROM overloaded;

----------------------------------------
-- sample queries for FIXED_REPEATER_UDF
----------------------------------------

-- valid calls
-- deterministic method
-- no constants involved -> each UDF call is made for every row and col it's applied to
-- here 4 input rows -> 4 x 4 -> 16 UDF calls in total
SELECT 
    FIXED_REPEATER_UDF(data1,2) as udf_output1,FIXED_REPEATER_UDF(data2,3) as udf_output2,
    FIXED_REPEATER_UDF(data3,1) as udf_output3,FIXED_REPEATER_UDF(data4,5) as udf_output4
FROM repeater;

-- calling it twice with 2 constants as input -> no calls
SELECT 
    id,FIXED_REPEATER_UDF('Austin',2) as udf_output1,FIXED_REPEATER_UDF(1809,3) as udf_output2
FROM repeater;

-----------------------------------------
-- sample queries for RANDOM_REPEATER_UDF
-----------------------------------------

-- valid calls
-- non-deterministic method
-- no constants involved -> each UDF call is made for every row and col it's applied to
-- here 4 input rows -> 4 x 4 -> 16 UDF calls in total
SELECT 
    id,RANDOM_REPEATER_UDF(data1,10) as udf_output1,RANDOM_REPEATER_UDF(data2,10) as udf_output2,
    RANDOM_REPEATER_UDF(data3,10) as udf_output3,RANDOM_REPEATER_UDF(data4,10) as udf_output4
FROM repeater;

-- calling it twice with 2 constants as input -> still 8 UDF calls due to non-deterministic nature
SELECT 
    id,RANDOM_REPEATER_UDF('Austin',10) as udf_output1,RANDOM_REPEATER_UDF(1809,5) as udf_output2
FROM repeater;







------------------------------------------------------------------------
-- queries without actual source table but static / constant expressions
------------------------------------------------------------------------


-------------------------------
-- sample queries for HELLO_UDF
-------------------------------

-- valid calls
SELECT 
    udf_output1
        FROM (
            VALUES
                (HELLO_UDF('Pedro','ES')),
                (HELLO_UDF('Hans-Peter','DE')),
                (HELLO_UDF('w o r l d')),
                (HELLO_UDF(null,'IT'))
        ) t1(udf_output1);

------------------------------
-- sample queries for CALC_UDF
------------------------------

-- valid calls
SELECT 
    udf_output1
        FROM (
            VALUES
                (CALC_UDF(ROW('ADD',12.0,34.0))),
                (CALC_UDF(ROW('SUBTRACT',10.0,10.0))),
                (CALC_UDF(ROW('MULTIPLY',23.0,42.0))),
                (CALC_UDF(ROW('DIVIDE',1024.0,256.0)))
        ) t1(udf_output1);

------------------------------------
-- sample queries for OVERLOADED_UDF
------------------------------------

-- valid calls

SELECT
    OVERLOADED_UDF(data1),OVERLOADED_UDF(data2,data3)
FROM (
SELECT
    data1,data2,data3
        FROM (
            VALUES
            ('foo',12.34,56.78)
        ) t1(data1,data2,data3)
);


------------------------------------
-- sample queries for REPEATER_UDF
------------------------------------

-- valid calls

SELECT 
    id,FIXED_REPEATER_UDF(data1,3) as udf_output1,FIXED_REPEATER_UDF(data2,5) as udf_output2
    FROM (
    SELECT
        id,data1,data2
            FROM (
                VALUES
                (1,'foo',234),
                (2,'bla',876)
            ) t1 (id,data1,data2)
) t2;


SELECT 
    id,FIXED_REPEATER_UDF('hello',3) as udf_output1,FIXED_REPEATER_UDF(data2,5) as udf_output2
    FROM (
    SELECT
        id,data1,data2
            FROM (
                VALUES
                (1,'foo',234),
                (2,'bla',876)
            ) t1 (id,data1,data2)
) t2;


SELECT 
    RANDOM_REPEATER_UDF('hello',3) as udf_output
    FROM (
    SELECT
        id,data1,data2
            FROM (
                VALUES
                (1,'foo',234),
                (2,'bla',876)
            ) t1 (id,data1,data2)
) t2;
