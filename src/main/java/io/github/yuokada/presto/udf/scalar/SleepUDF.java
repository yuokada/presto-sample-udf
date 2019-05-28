package io.github.yuokada.presto.udf.scalar;

import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;

import static io.prestosql.spi.type.StandardTypes.INTEGER;

public class SleepUDF
{
    @Description("Sleep n seconds")
    @ScalarFunction("sleep")
    @SqlType(INTEGER)
    public static long sleep(@SqlType(INTEGER) long t)
    {
        try {
            Thread.sleep(t * 1000);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        return t;
    }
}
