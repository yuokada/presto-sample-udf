package io.github.yuokada.presto.udf.scalar;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;

import static io.airlift.slice.Slices.utf8Slice;

public class HelloWorldUDF
{
    private HelloWorldUDF()
    {
    }

    @Description("Hello World(UDF Practice)")
    @ScalarFunction("hello_world")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice helloworld(@SqlNullable @SqlType(StandardTypes.VARCHAR) Slice name)
    {
        if (name == null || name.toStringUtf8().isEmpty()) {
            return utf8Slice("Hello World");
        }
        else {
            return utf8Slice(String.format("Hello %s", name.toStringUtf8()));
        }
    }
}
