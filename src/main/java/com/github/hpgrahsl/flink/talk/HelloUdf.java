package com.github.hpgrahsl.flink.talk;

import java.util.Map;

import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloUdf extends ScalarFunction {
    
    private static final transient Logger LOGGER = LoggerFactory.getLogger(HelloUdf.class);

    static Map<String,String> GREETINGS = 
        Map.of(
            "EN","Hello",
            "DE","Hallo",
            "FR","Bonjour",
            "ES","Hola",
            "IT","Ciao"
        );

    // * scalar functions expect at least one public eval() method
    // * automatic type inference based on reflection
    // * method overloading for eval is supported
    
    //   -> input param 1 type STRING
    //   -> input param 2 type STRING
    //   <- output type STRING
    public String eval(String who, String lang) {
        LOGGER.info("eval call with who: {}, lang: {}",who,lang);
        return sayHello(who, lang);
    }

    //   -> input param 1 type STRING
    //   <- output type STRING
    public String eval(String who) {
        LOGGER.info("eval call with who: {}",who);
        return sayHello(who, null);
    }

    //   -> no input param
    //   <- output type STRING
    public String eval() {
        LOGGER.info("eval call without params");
        return sayHello(null, null);
    }

    // actual function logic implemented separately, here by means of a static helper method
    static String sayHello(String who, String lang) {
        var greeting = GREETINGS.getOrDefault(
            lang == null ? "EN" : lang,
            GREETINGS.get("EN")
        );
        return greeting + " " + (who == null ? "UDF" : who) + "!";
    }
    
}
