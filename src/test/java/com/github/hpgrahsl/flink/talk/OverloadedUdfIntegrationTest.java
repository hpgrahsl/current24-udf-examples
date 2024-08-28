package com.github.hpgrahsl.flink.talk;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import java.util.ArrayList;

import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.RegisterExtension;

@EnabledIfSystemProperty(named="many.overloadings",matches="true")
public class OverloadedUdfIntegrationTest {

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
        ENV.setParallelism(2);
        T_ENV = StreamTableEnvironment.create(ENV, EnvironmentSettings.newInstance().inStreamingMode().build());
        T_ENV.createTemporaryFunction("OVERLOADED_UDF", OverloadedUdf.class);
    }

    @AfterEach
    void cleanUpTemporaryResources() {
        T_ENV.dropTemporaryView("input_table");
    }

    @Test
    public void testOverloadedUdfWithValidInputs() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(
                DataTypes.FIELD("myString",DataTypes.STRING()),
                DataTypes.FIELD("myDouble1",DataTypes.DOUBLE().notNull()),
                DataTypes.FIELD("myDouble2",DataTypes.DOUBLE().notNull())
            ),
            Row.of("hello",12.34,56.78)
        );
        
        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery("SELECT OVERLOADED_UDF(myString) AS udf_output1,OVERLOADED_UDF(myDouble1,myDouble2) AS udf_output2 FROM input_table");
        var outputStream = T_ENV.toDataStream(outputTable);
        
        List<List<String>> results = new ArrayList<>();
        try (CloseableIterator<Row> rowCloseableIterator = outputStream.executeAndCollect()) {
            rowCloseableIterator.forEachRemaining(
                r -> {
                    List<String> strings = new ArrayList<>();
                    strings.add(r.<String>getFieldAs("udf_output1"));
                    strings.add(r.<String>getFieldAs("udf_output2"));
                    results.add(strings);
                }
            );
        }

        System.out.println(results);

        assertThat(results, containsInAnyOrder(List.of("overloading 1 for String: hello","overloading 14 for Double,Double (nullable): 12.34 | 56.78")));
    }

}
