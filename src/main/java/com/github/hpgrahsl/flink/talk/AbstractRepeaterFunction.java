package com.github.hpgrahsl.flink.talk;

import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Collectors;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.InputTypeStrategies;
import org.apache.flink.table.types.inference.TypeInference;

public abstract class AbstractRepeaterFunction extends ScalarFunction {

    public <T> String repeat(final T data, int length) {
        if (data == null) {
            return null;
        }
        if (length < 1) {
            return "";
        }
        return IntStream.range(0, length)
            .mapToObj(i -> String.valueOf(data))
            .collect(Collectors.joining(","));
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(InputTypeStrategies.or(
                    InputTypeStrategies.sequence(InputTypeStrategies.ANY,InputTypeStrategies.explicit(DataTypes.INT())),
                    InputTypeStrategies.sequence(InputTypeStrategies.ANY)
                ))
                .outputTypeStrategy(ctx -> Optional.of(DataTypes.STRING()))
                .build();
    }

}
