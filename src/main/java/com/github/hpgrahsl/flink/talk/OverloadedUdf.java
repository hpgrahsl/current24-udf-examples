package com.github.hpgrahsl.flink.talk;

import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OverloadedUdf extends ScalarFunction {

    private static final transient Logger LOGGER = LoggerFactory.getLogger(OverloadedUdf.class);

    /* NOTE:
        - just 14 overloadings for a trivial eval(...) method take the integration test > 1 min
        - !! adding a few more overloadings makes it hang (almost) 'indefinitely' !!
    */

    public String eval(String myString) {
        LOGGER.info("eval call with String: {}",myString);
        return "overloading 1 for String: " + myString;
    }

    public String eval(Boolean myBoolean) {
        LOGGER.info("eval call with Boolean: {}",myBoolean);
        return "overloading 2 for Boolean (nullable): " + myBoolean;
    }

    public String eval(Short myShort) {
        LOGGER.info("eval call with Short: {}",myShort);
        return "overloading 3 for Short (nullable): " + myShort;
    }
    
    public String eval(Integer myInteger) {
        LOGGER.info("eval call with Integer: {}",myInteger);
        return "overloading 4 for Integer (nullable): " + myInteger;
    }

    public String eval(Long myLong) {
        LOGGER.info("eval call with Long: {}",myLong);
        return "overloading 5 for Long (nullable): " + myLong;
    }
    
    public String eval(Float myFloat) {
        LOGGER.info("eval call with Float: {}",myFloat);
        return "overloading 6 for Float (nullable): " + myFloat;
    }
    
    public String eval(Double myDouble) {
        LOGGER.info("eval call with Double: {}",myDouble);
        return "overloading 7 for Double (nullable): " + myDouble;
    }
    
    public String eval(String myString1, String myString2) {
        LOGGER.info("eval call with String: {},String: {}",myString1,myString2);
        return "overloading 8 for String,String: " + myString1 + " | " + myString2;
    }

    public String eval(Boolean myBoolean1, Boolean myBoolean2) {
        LOGGER.info("eval call with Boolean: {},Boolean: {}",myBoolean1,myBoolean2);
        return "overloading 9 for Boolean,Boolean (nullable): " + myBoolean1 + " | " + myBoolean2;
    }

    public String eval(Short myShort1, Short myShort2) {
        LOGGER.info("eval call with Short: {},Short: {}",myShort1,myShort2);
        return "overloading 10 for Short,Short (nullable): " + myShort1 + " | " + myShort2;
    }
    
    public String eval(Integer myInteger1, Integer myInteger2) {
        LOGGER.info("eval call with Integer: {},Integer: {}",myInteger1,myInteger2);
        return "overloading 11 for Integer,Integer (nullable): " + myInteger1 + " | " + myInteger2;
    }
    
    public String eval(Long myLong1, Long myLong2) {
        LOGGER.info("eval call with Long: {},Long: {}",myLong1,myLong2);
        return "overloading 12 for Long,Long (nullable): " + myLong1 + " | " + myLong2;
    }
    
    public String eval(Float myFloat1, Float myFloat2) {
        LOGGER.info("eval call with Float: {},Float: {}",myFloat1,myFloat2);
        return "overloading 13 for Float,Float (nullable): " + myFloat1 + " | " + myFloat2;
    }
    
    public String eval(Double myDouble1, Double myDouble2) {
        LOGGER.info("eval call with Double: {},Double: {}",myDouble1,myDouble2);
        return "overloading 14 for Double,Double (nullable): " + myDouble1 + " | " + myDouble2;
    }
    
}
