package com.github.hpgrahsl.flink.talk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertAll;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

public class RepeaterUdfTest {

    @Test
    void testFixedRepeaterUdfWithValidInputs() {
        var udf = new FixedRepeaterUdf();
        assertAll(
            () -> assertEquals("foo & blah,foo & blah", udf.eval("foo & blah", 2)),
            () -> assertEquals("42,42,42,42,42", udf.eval(42,5)),
            () -> assertEquals("true", udf.eval(true,1))
        );
    }

    @Test
    void testRandomRepeaterUdfWithValidInputs() {
        var udf = new RandomRepeaterUdf();
        assertAll(
            () -> assertField("foo & blah", 100, udf.eval("foo & blah", 100)),
            () -> assertField(42, 50, udf.eval(42, 50)),
            () -> assertField(true, 25, udf.eval(true, 25))
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
