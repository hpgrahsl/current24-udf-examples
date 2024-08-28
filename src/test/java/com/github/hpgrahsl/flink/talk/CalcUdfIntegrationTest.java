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
import org.junit.jupiter.api.extension.RegisterExtension;

public class CalcUdfIntegrationTest {

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
        T_ENV.createTemporaryFunction("CALC_UDF", CalcUdf.class);
    }

    @AfterEach
    void cleanUpTemporaryResources() {
        T_ENV.dropTemporaryView("input_table");
    }

    @Test
    public void testCalcUdfWithValidInputs() throws Exception {
        var inputTable = T_ENV.fromValues(
            DataTypes.ROW(
                DataTypes.FIELD("calculation",DataTypes.ROW(DataTypes.STRING(),DataTypes.DOUBLE(),DataTypes.DOUBLE()))
            ),
            Row.of(Row.of(CalcUdf.Operation.ADD.name(),12.0,34.0)),
            Row.of(Row.of(CalcUdf.Operation.SUBTRACT.name(),10.0,10.0)),
            Row.of(Row.of(CalcUdf.Operation.MULTIPLY.name(),23.0,42.0)),
            Row.of(Row.of(CalcUdf.Operation.DIVIDE.name(),1024.0,256.0))
        );
        
        T_ENV.createTemporaryView("input_table", inputTable);
        var outputTable = T_ENV.sqlQuery("SELECT CALC_UDF(calculation) AS udf_output FROM input_table");
        var outputStream = T_ENV.toDataStream(outputTable);
        
        List<Double> results = new ArrayList<>();
        try (CloseableIterator<Row> rowCloseableIterator = outputStream.executeAndCollect()) {
            rowCloseableIterator.forEachRemaining(r -> results.add(r.<Double>getFieldAs("udf_output")));
        }

        assertThat(results, containsInAnyOrder(46.0,0.0,966.0,4.0));
    }

}
