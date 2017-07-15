package io.github.yuokada.presto.udf;

import com.facebook.presto.spi.Plugin;
import com.google.common.collect.ImmutableSet;
import io.github.yuokada.presto.udf.scalar.HelloWorldUDF;

import java.util.Set;

public class UdfPlugin
        implements Plugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(HelloWorldUDF.class)
                .build();
    }
}
