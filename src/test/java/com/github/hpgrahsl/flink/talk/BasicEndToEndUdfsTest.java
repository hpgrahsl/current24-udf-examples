package com.github.hpgrahsl.flink.talk;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.File;
import java.util.List;
import java.util.ArrayList;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class BasicEndToEndUdfsTest {

    private static final String NAME_SERVICE_MYSQL = "mysql";
    private static final String NAME_SERVICE_FLINK_JOB_MANAGER = "jobmanager";
    private static final int PORT_MYSQL = 3306;
    private static final int PORT_FLINK_JOB_MANAGER = 8081;

    @Container
    static ComposeContainer COMPOSE_CONTAINER = 
        new ComposeContainer(new File("src/test/resources/compose-flink-test.yaml"))
            .withExposedService(NAME_SERVICE_MYSQL, PORT_MYSQL, Wait.forHealthcheck())
            .withExposedService(NAME_SERVICE_FLINK_JOB_MANAGER, PORT_FLINK_JOB_MANAGER, Wait.forListeningPort())
            .withLocalCompose(true);
        
    static StreamExecutionEnvironment ENV;
    static StreamTableEnvironment T_ENV;

    @BeforeAll
    static void setUp() {
        ENV = StreamExecutionEnvironment
                .createRemoteEnvironment(
                    "localhost",
                    COMPOSE_CONTAINER.getServicePort(NAME_SERVICE_FLINK_JOB_MANAGER,PORT_FLINK_JOB_MANAGER)
                );
        ENV.setParallelism(1);
        T_ENV = StreamTableEnvironment.create(ENV, EnvironmentSettings.newInstance().inStreamingMode().build());
        T_ENV.createTemporaryFunction("HELLO_UDF", HelloUdf.class);
        T_ENV.createTemporaryFunction("CALC_UDF", CalcUdf.class);
        T_ENV.createTemporaryFunction("OVERLOADED_UDF", OverloadedUdf.class);
    }

    @Test
    void helloUdfTest() throws Exception {
        T_ENV.executeSql(
            """
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
            """
        );
        try (var iterator = T_ENV.executeSql(
            """
                SELECT
                    HELLO_UDF(who,lang) AS udf_output
                FROM hello;
            """
        ).collect()) {
            List<String> results = collectFieldsFromRowIterator(iterator,"udf_output",4);
            assertThat(results, containsInAnyOrder("Hallo Hans-Peter!","Hola Pedro!","Hello w o r l d!","Ciao UDF!"));    
        }
    }

    @Test
    void calcUdfTest() throws Exception {
        T_ENV.executeSql(
            """
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
            """
        );
        try (var iterator = T_ENV.executeSql(
            """
                SELECT 
                    CALC_UDF(ROW(operation,operand1,operand2)) as udf_output
                FROM calc;
            """
        ).collect()) {
            List<Double> results = collectFieldsFromRowIterator(iterator,"udf_output",4);
            assertThat(results, containsInAnyOrder(46.0,0.0,966.0,4.0));    
        }
    }

    @Test
    @EnabledIfSystemProperty(named="many.overloadings",matches="true")
    void overloadedUdfTest() throws Exception {
        T_ENV.executeSql(
            """
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
                    'jdbc.properties.maxAllowedPacket' = '16777216',
                    'server-time-zone' = 'UTC',
                    'database-name' = 'udf_demo',
                    'table-name' = 'overloaded'
                );
            """
        );
        try (var iterator = T_ENV.executeSql(
            """
                SELECT
                    OVERLOADED_UDF(data1) AS udf_output1,
                    OVERLOADED_UDF(data2) AS udf_output2,
                    OVERLOADED_UDF(data3) AS udf_output3
                FROM overloaded;
            """
        ).collect()) {
            List<List<String>> results = collectFieldsFromRowIterator(iterator,List.of("udf_output1","udf_output2","udf_output3"),4);
            assertThat(results,containsInAnyOrder(
                List.of(
                    "overloading 1 for String: hello",
                    "overloading 4 for Integer (nullable): 23",
                    "overloading 2 for Boolean (nullable): false"
                ),
                List.of(
                    "overloading 1 for String: current",
                    "overloading 4 for Integer (nullable): 0",
                    "overloading 2 for Boolean (nullable): true"
                ),
                List.of(
                    "overloading 1 for String: apache",
                    "overloading 4 for Integer (nullable): 99",
                    "overloading 2 for Boolean (nullable): true"
                ),
                List.of(
                    "overloading 1 for String: flink",
                    "overloading 4 for Integer (nullable): 0",
                    "overloading 2 for Boolean (nullable): false"
                )
            ));
        }
    }

    private static <T> List<T> collectFieldsFromRowIterator(CloseableIterator<Row> iterator, String fieldName, int numRows) {
        List<T> results = new ArrayList<>();
        while(iterator.hasNext()) {
            var row = iterator.next();
            results.add(row.<T>getFieldAs(fieldName));
            if(results.size() == numRows) {
                break;
            }
        }
        return results;
    }

    private static <T> List<List<T>> collectFieldsFromRowIterator(CloseableIterator<Row> iterator, List<String> fieldNames, int rowLimit) {
        List<List<T>> rows = new ArrayList<>();
        while(iterator.hasNext()) {
            var r = iterator.next();
            var columns = new ArrayList<T>();
            for (var f:fieldNames) {
                columns.add(r.<T>getFieldAs(f));
            }
            rows.add(columns);
            if(rows.size() == rowLimit) {
                break;
            }
        }
        return rows;
    }

}
