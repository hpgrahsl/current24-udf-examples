package com.github.hpgrahsl.flink.talk;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;

import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class HelloUdfIntegrationTest {

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(2)
                    .setNumberSlotsPerTaskManager(2)
                    //.setConfiguration(new Configuration())
                    .build());

    static StreamExecutionEnvironment ENV;
    static StreamTableEnvironment T_ENV;

    @BeforeAll
    static void setUp(@InjectMiniCluster MiniCluster miniCluster) {
        ENV = StreamExecutionEnvironment.getExecutionEnvironment();
        ENV.setParallelism(4);
        T_ENV = StreamTableEnvironment.create(ENV, EnvironmentSettings.newInstance().inStreamingMode().build());
        T_ENV.createTemporaryFunction("HELLO_UDF", HelloUdf.class);
    }

    @AfterEach
    void cleanUpTemporaryResources() {
        T_ENV.dropTemporaryView("input_table");
    }

    @Test
    public void testHelloUdfWithValidInputs() throws Exception {
        var inputTable = T_ENV.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("who", DataTypes.STRING()),
                        DataTypes.FIELD("lang", DataTypes.STRING())),
                Row.of("Hans-Peter", "DE"),
                Row.of("Pedro", "ES"),
                Row.of("w o r l d", null),
                Row.of(null, "IT"));

        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery("SELECT HELLO_UDF(who,lang) AS udf_output FROM input_table");
        var outputStream = T_ENV.toDataStream(outputTable);

        List<String> results = new ArrayList<>();
        try (CloseableIterator<Row> rowCloseableIterator = outputStream.executeAndCollect()) {
            rowCloseableIterator.forEachRemaining(r -> results.add(r.<String>getFieldAs("udf_output")));
        }

        assertThat(results, containsInAnyOrder("Hallo Hans-Peter!", "Hola Pedro!", "Hello w o r l d!", "Ciao UDF!"));
    }

    @Test
    public void testHelloUdfWithInvalidInputType() throws Exception {
        var inputTable = T_ENV.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("param1", DataTypes.STRING()),
                        DataTypes.FIELD("param2", DataTypes.INT())),
                Row.of("wrong type for param 2", 1));

        T_ENV.createTemporaryView("input_table", inputTable);
        assertThrows(ValidationException.class,
                () -> T_ENV.sqlQuery("SELECT HELLO_UDF(param1,param2) AS udf_output FROM input_table"));

    }

    @Test
    public void testHelloUdfWithAddtionalParam() throws Exception {
        var inputTable = T_ENV.fromValues(
                DataTypes.ROW(
                        DataTypes.FIELD("param1", DataTypes.STRING()),
                        DataTypes.FIELD("param2", DataTypes.STRING()),
                        DataTypes.FIELD("param3", DataTypes.INT())),
                Row.of("param 1", "param 2", 3));

        T_ENV.createTemporaryView("input_table", inputTable);
        assertThrows(ValidationException.class,
                () -> T_ENV.sqlQuery("SELECT HELLO_UDF(param1,param2,param3) AS udf_output FROM input_table"));

    }

}
