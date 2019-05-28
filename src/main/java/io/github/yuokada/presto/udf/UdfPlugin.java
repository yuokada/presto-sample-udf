package io.github.yuokada.presto.udf;

import com.google.common.collect.ImmutableSet;
import io.github.yuokada.presto.udf.scalar.HelloWorldScalaUDF;
import io.github.yuokada.presto.udf.scalar.HelloWorldUDF;
import io.github.yuokada.presto.udf.scalar.KuromojiUDF;
import io.prestosql.spi.Plugin;

import java.util.Set;

public class UdfPlugin
        implements Plugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(HelloWorldUDF.class)
                .add(HelloWorldScalaUDF.class)
                .add(KuromojiUDF.class)
                .build();
    }
}
