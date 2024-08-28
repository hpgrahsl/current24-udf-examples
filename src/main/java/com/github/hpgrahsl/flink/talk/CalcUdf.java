package com.github.hpgrahsl.flink.talk;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CalcUdf extends ScalarFunction {

    private static final transient Logger LOGGER = LoggerFactory.getLogger(CalcUdf.class);
    
    public static enum Operation {
        ADD,
        SUBTRACT,
        MULTIPLY,
        DIVIDE
    }

    // * scalar functions expect at least one public eval() method
    // * NO automatic type inference due to Row type being used as input param
    
    //   -> input param 1 type Row (! NEEDS @DataTypeHint to infer of field types of Row !)
    //   <- output type DOUBLE
    public Double eval(@DataTypeHint("ROW<f0 STRING,f1 DOUBLE, f2 DOUBLE>") Row calculation) {
        LOGGER.info("eval call with data: {}",calculation);
        var op = calculation.<String>getFieldAs(0);
        var num1 = calculation.<Double>getFieldAs(1);
        var num2 = calculation.<Double>getFieldAs(2);
        return doCalc(op, num1,num2);
    }

    // actual function logic implemented separately, here by means of a static helper method
    static Double doCalc(String op, Double num1, Double num2) {
        var operation = Operation.valueOf(op);
        return switch (operation) {
            case ADD -> num1 + num2;
            case SUBTRACT -> num1 - num2;
            case MULTIPLY -> num1 * num2;
            case DIVIDE -> num1 / num2;
        };
    }
    
}
