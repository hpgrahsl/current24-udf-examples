package com.github.hpgrahsl.flink.talk;

import org.apache.flink.table.functions.ScalarFunction;

public class NoEvalUdf extends ScalarFunction {
    
    public String noEval() {
        return "no proper eval() method";
    }

}
