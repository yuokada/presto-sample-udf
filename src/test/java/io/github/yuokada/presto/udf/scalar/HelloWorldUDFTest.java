package io.github.yuokada.presto.udf.scalar;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

/**
 * Unit test for UDF.
 */
public class HelloWorldUDFTest
        extends AbstractTestFunctions
{

    @BeforeClass
    public void setUp()
    {
        registerScalar(HelloWorldUDF.class);
    }

    @Test
    public void testHelloWorld()
            throws Exception
    {
        assertFunction("hello_world('')", VARCHAR, "Hello World");
    }

    @Test
    public void testHelloJohn()
            throws Exception
    {
        assertFunction("hello_world('John')", VARCHAR, "Hello John");
    }
}
