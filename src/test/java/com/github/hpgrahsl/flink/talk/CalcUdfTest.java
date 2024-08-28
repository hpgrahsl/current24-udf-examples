package com.github.hpgrahsl.flink.talk;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.flink.types.Row;
import org.junit.jupiter.api.Test;

public class CalcUdfTest {

    @Test
    void testCalcUdfWithValidInputs() {
        var udf = new CalcUdf();
        assertAll(
            () -> assertEquals(46.0,udf.eval(Row.of(CalcUdf.Operation.ADD.name(),12.0,34.0))),
            () -> assertEquals(0.0,udf.eval(Row.of(CalcUdf.Operation.SUBTRACT.name(),10.0,10.0))),
            () -> assertEquals(966.0,udf.eval(Row.of(CalcUdf.Operation.MULTIPLY.name(),23.0,42.0))),
            () -> assertEquals(4.0,udf.eval(Row.of(CalcUdf.Operation.DIVIDE.name(),1024.0,256.0)))
        );
    }

    @Test
    void testCalcUdfWithInvalidParams() {
        var udf = new CalcUdf();
        assertAll(
            () -> assertThrows(IllegalArgumentException.class,() -> udf.eval(Row.of("UNKNOWN",1.0,99.0))),
            () -> assertThrows(ClassCastException.class,() -> udf.eval(Row.of(CalcUdf.Operation.ADD.name(),"foo","blah"))),
            () -> assertThrows(ArrayIndexOutOfBoundsException.class,() -> udf.eval(Row.of()))
        );
    }
    
}
