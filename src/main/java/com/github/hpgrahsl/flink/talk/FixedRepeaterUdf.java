package com.github.hpgrahsl.flink.talk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FixedRepeaterUdf extends AbstractRepeaterFunction {

    private static final transient Logger LOGGER = LoggerFactory.getLogger(FixedRepeaterUdf.class);
    private static int COUNTER = 0;
    private static final int DEFAULT_LENGTH = 10;

    //NOTE: defaults to true, but making it explicitly visible here :)
    @Override
    public boolean isDeterministic() {
        return true;
    }

    public <T> String eval(final T data, int length) {
        COUNTER++;
        LOGGER.info("eval call #{} with data: {} and {} repetition(s)",COUNTER,data,length);
        return repeat(data, length);
    }

    public <T> String eval(final T data) {
        COUNTER++;
        LOGGER.info("eval call #{} with data: {} and {} repetition(s)",COUNTER,data,DEFAULT_LENGTH);
        return repeat(data, DEFAULT_LENGTH);
    }

}
