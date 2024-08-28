package com.github.hpgrahsl.flink.talk;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
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
import org.junit.jupiter.api.extension.RegisterExtension;

public class RepeaterUdfIntegrationTest {

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
        T_ENV.createTemporaryFunction("FIXED_REPEATER_UDF", FixedRepeaterUdf.class);
        T_ENV.createTemporaryFunction("RANDOM_REPEATER_UDF", RandomRepeaterUdf.class);
    }

    @AfterEach
    void cleanUpTemporaryResources() {
        T_ENV.dropTemporaryView("input_table");
    }

    @Test
    public void testFixedRepeaterUdf() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(
                DataTypes.FIELD("data1",DataTypes.STRING()),
                DataTypes.FIELD("data2",DataTypes.DOUBLE()),
                DataTypes.FIELD("data3",DataTypes.INT()),
                DataTypes.FIELD("data4",DataTypes.BOOLEAN())
            ),
            Row.of("hello",23.0,42,false)
        );
        
        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
                """
                    SELECT 
                        FIXED_REPEATER_UDF(data1,2) as udf_output1,
                        FIXED_REPEATER_UDF(data2,3) as udf_output2,
                        FIXED_REPEATER_UDF(data3,1) as udf_output3,
                        FIXED_REPEATER_UDF(data4,5) as udf_output4
                    FROM input_table
                """
        );
        
        var outputStream = T_ENV.toDataStream(outputTable);    
        List<List<String>> results = new ArrayList<>();
        try (CloseableIterator<Row> rowCloseableIterator = outputStream.executeAndCollect()) {
            rowCloseableIterator.forEachRemaining(
                r -> {
                    List<String> strings = new ArrayList<>();
                    strings.add(r.<String>getFieldAs("udf_output1"));
                    strings.add(r.<String>getFieldAs("udf_output2"));
                    strings.add(r.<String>getFieldAs("udf_output3"));
                    strings.add(r.<String>getFieldAs("udf_output4"));
                    results.add(strings);
                }
            );
        }

        assertAll(
            () -> assertAll(
                () -> assertEquals("hello,hello", results.get(0).get(0)),
                () -> assertEquals("23.0,23.0,23.0", results.get(0).get(1)),
                () -> assertEquals("42", results.get(0).get(2)),
                () -> assertEquals("false,false,false,false,false", results.get(0).get(3))
            )
        );
    }

    @Test
    public void testRandomRepeaterUdf() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(
                DataTypes.FIELD("data1",DataTypes.STRING()),
                DataTypes.FIELD("data2",DataTypes.DOUBLE()),
                DataTypes.FIELD("data3",DataTypes.INT()),
                DataTypes.FIELD("data4",DataTypes.BOOLEAN())
            ),
            Row.of("hello",23.0,42,false)
        );
        
        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery(
                """
                    SELECT 
                        RANDOM_REPEATER_UDF(data1,5) as udf_output1,
                        RANDOM_REPEATER_UDF(data2,10) as udf_output2,
                        RANDOM_REPEATER_UDF(data3,10) as udf_output3,
                        RANDOM_REPEATER_UDF(data4,5) as udf_output4
                    FROM input_table
                """
        );
        
        var outputStream = T_ENV.toDataStream(outputTable);    
        List<List<String>> results = new ArrayList<>();
        try (CloseableIterator<Row> rowCloseableIterator = outputStream.executeAndCollect()) {
            rowCloseableIterator.forEachRemaining(
                r -> {
                    List<String> strings = new ArrayList<>();
                    strings.add(r.<String>getFieldAs("udf_output1"));
                    strings.add(r.<String>getFieldAs("udf_output2"));
                    strings.add(r.<String>getFieldAs("udf_output3"));
                    strings.add(r.<String>getFieldAs("udf_output4"));
                    results.add(strings);
                }
            );
        }
        
        assertAll(
            () -> assertAll(
                () -> assertField("hello", 5, results.get(0).get(0)),
                () -> assertField(23.0, 10, results.get(0).get(1)),
                () -> assertField(42, 10, results.get(0).get(2)),
                () -> assertField(false, 5, results.get(0).get(3))
            )
        );
    }

    static void assertField(Object data, int bound, String actual) {
        var array = actual.split("\\,");
        var expected = new String[array.length];
        Arrays.fill(expected, String.valueOf(data));
        assertAll(
            () -> assertTrue(array.length >= 1 && array.length <= bound),
            () -> assertArrayEquals(expected, array)
        );
    }

}
