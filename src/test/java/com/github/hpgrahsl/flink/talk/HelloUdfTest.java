package com.github.hpgrahsl.flink.talk;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class HelloUdfTest {

    @Test
    void testHelloUdfWithValidInputs() {
        var udf = new HelloUdf();
        assertAll(
            () -> assertEquals("Hello UDF!",udf.eval()),
            () -> assertEquals("Hello Current'24!",udf.eval("Current'24")),
            () -> assertEquals("Hola Developers!",udf.eval("Developers","ES")),
            () -> assertEquals("Hello Current'24!",udf.eval("Current'24",null)),
            () -> assertEquals("Hello UDF!",udf.eval(null,null))
        );
    }
    
}
