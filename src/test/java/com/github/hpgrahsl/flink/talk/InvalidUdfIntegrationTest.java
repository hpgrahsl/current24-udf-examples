package com.github.hpgrahsl.flink.talk;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class InvalidUdfIntegrationTest {

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_RESOURCE = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(2)
                    .setNumberSlotsPerTaskManager(2)
                    //.setConfiguration(new Configuration())
                    .build());

    static StreamExecutionEnvironment ENV;
    static StreamTableEnvironment T_ENV;

    @Test
    void registerInvalidUDFsTest(@InjectMiniCluster MiniCluster miniCluster) {
        ENV = StreamExecutionEnvironment.getExecutionEnvironment();
        ENV.setParallelism(4);
        T_ENV = StreamTableEnvironment.create(ENV, EnvironmentSettings.newInstance().inStreamingMode().build());
        assertAll(
            () -> assertThrows(ValidationException.class, () -> T_ENV.createTemporaryFunction("NO_EVAL_UDF", NoEvalUdf.class)),
            () -> assertThrows(ValidationException.class, () -> T_ENV.createTemporaryFunction("WRONG_EVAL_UDF", WrongEvalUdf.class))
        );
    }

}
