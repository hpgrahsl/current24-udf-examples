ADD JAR '/opt/flink/lib/flink-udf-examples-1.0.0.jar';

-- define functions 
CREATE FUNCTION hello_udf AS 'com.github.hpgrahsl.flink.talk.HelloUdf' LANGUAGE JAVA;
CREATE FUNCTION calc_udf AS 'com.github.hpgrahsl.flink.talk.CalcUdf' LANGUAGE JAVA;
CREATE FUNCTION overloaded_udf AS 'com.github.hpgrahsl.flink.talk.OverloadedUdf' LANGUAGE JAVA;
CREATE FUNCTION fixed_repeater_udf AS 'com.github.hpgrahsl.flink.talk.FixedRepeaterUdf' LANGUAGE JAVA;
CREATE FUNCTION random_repeater_udf AS 'com.github.hpgrahsl.flink.talk.RandomRepeaterUdf' LANGUAGE JAVA;

-- define source tables 
CREATE TABLE hello (
    id int,
    who varchar(255),
    lang char(2),
    PRIMARY KEY (id) NOT ENFORCED
)  WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = 'sECreT',
    'server-time-zone' = 'UTC',
    'database-name' = 'udf_demo',
    'table-name' = 'hello'
);

CREATE TABLE calc (
    id int,
    operation varchar(255),
    operand1 double,
    operand2 double,
    PRIMARY KEY (id) NOT ENFORCED
)  WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = 'sECreT',
    'server-time-zone' = 'UTC',
    'database-name' = 'udf_demo',
    'table-name' = 'calc'
);

CREATE TABLE overloaded (
    id int,
    data1 varchar(255),
    data2 int,
    data3 boolean,
    PRIMARY KEY (id) NOT ENFORCED
)  WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = 'sECreT',
    'server-time-zone' = 'UTC',
    'database-name' = 'udf_demo',
    'table-name' = 'overloaded'
);

CREATE TABLE repeater (
    id int,
    data1 varchar(255),
    data2 float,
    data3 int,
    data4 boolean,
    PRIMARY KEY (id) NOT ENFORCED
)  WITH (
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = 'root',
    'password' = 'sECreT',
    'server-time-zone' = 'UTC',
    'database-name' = 'udf_demo',
    'table-name' = 'repeater'
);
