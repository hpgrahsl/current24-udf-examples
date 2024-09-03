package com.github.hpgrahsl.flink.talk;

import org.apache.flink.table.functions.ScalarFunction;

public class WrongEvalUdf extends ScalarFunction {
    
    String eval() {
        return "non-public eval() method";
    }

}
