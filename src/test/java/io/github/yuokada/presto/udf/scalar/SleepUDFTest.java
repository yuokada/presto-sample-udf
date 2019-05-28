package io.github.yuokada.presto.udf.scalar;

import io.prestosql.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.IntegerType.INTEGER;

public class SleepUDFTest
        extends AbstractTestFunctions
{

    @BeforeClass
    public void setUp()
    {
        registerScalar(SleepUDF.class);
    }

    @Test
    public void testHelloWorld()
            throws Exception
    {
        assertFunction("sleep(1)", INTEGER, 1);
        assertFunction("sleep(3)", INTEGER, 3);
    }
}
