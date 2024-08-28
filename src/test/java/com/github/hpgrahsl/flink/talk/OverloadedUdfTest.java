package com.github.hpgrahsl.flink.talk;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class OverloadedUdfTest {

    @Test
    void testOverloadedUdfWithValidInputs() {
        var udf = new OverloadedUdf();
        assertAll(
            () -> assertEquals("overloading 1 for String: hello",udf.eval("hello")),
            //... 
            () -> assertEquals("overloading 14 for Double,Double (nullable): 12.34 | 56.78",udf.eval(12.34,56.78))
        );
    }
    
}
