package com.github.hpgrahsl.flink.talk;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import javax.annotation.Nullable;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class HelloUdfTest {

    @Test
    @DisplayName("verify calling HELLO_UDF() = 'Hello UDF!'")
    void testHelloUdfWithoutParam() {
        var udf = new HelloUdf();
        assertEquals("Hello UDF!",udf.eval());
    }

    @ParameterizedTest(name = "verify calling HELLO_UDF[''{0}''] = ''{1}''")
    @CsvSource({
        "world,         Hello world!",
        "Current'24,    Hello Current'24!",
        ",              Hello UDF!"
    })
    @DisplayName("verify HELLO_UDF(who) = result")
    void testHelloUdfWithOneParam(String who, String result) {
        var udf = new HelloUdf();
        assertEquals(result,udf.eval(who));
    }

    @ParameterizedTest(name = "verify calling HELLO_UDF[''{0}'',''{1}''] = ''{2}''")
    @CsvSource({
        "developers,    ES, Hola developers!",
        "Current'24,,       Hello Current'24!",
        ",              IT, Ciao UDF!",
        ",,                 Hello UDF!"
    })
    @DisplayName("verify HELLO_UDF(who) = result")
    void testHelloUdfWithTwoParams(@Nullable String who, String lang, String result) {
        var udf = new HelloUdf();
        assertEquals(result,udf.eval(who,lang));
    }

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
