package io.github.yuokada.presto.udf.scalar;

import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.spi.type.Type;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

/**
 * Unit test for UDF.
 */
public class HelloWorldUDFTest
{

    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        functionAssertions = new FunctionAssertions().addScalarFunctions(HelloWorldUDF.class);
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

    private void assertFunction(String projection, Type expectedType, Object expected)
    {
        functionAssertions.assertFunction(projection, expectedType, expected);
    }
}
