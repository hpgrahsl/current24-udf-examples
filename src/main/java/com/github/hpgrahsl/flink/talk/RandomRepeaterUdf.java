package com.github.hpgrahsl.flink.talk;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RandomRepeaterUdf extends AbstractRepeaterFunction {

    private static final transient Logger LOGGER = LoggerFactory.getLogger(RandomRepeaterUdf.class);
    private static final Random RANDOM = new Random();
    private static int COUNTER = 0;

    @Override
    public boolean isDeterministic() {
        return false;
    }

    public <T> String eval(final T data, int upperBound) {
        COUNTER++;
        var length = 1 + RANDOM.nextInt(upperBound);
        LOGGER.info("eval call #{} with data: {} and {} repetition(s)",COUNTER,data,length);
        return repeat(data, length);
    }

}
