package io.github.yuokada.presto.udf.scalar;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

/**
 * Unit test for UDF.
 */
public class HelloWorldScalaUDFTest
        extends AbstractTestFunctions
{

    @BeforeClass
    public void setUp()
    {
        registerScalar(HelloWorldScalaUDF.class);
    }

    @Test
    public void testHelloWorld()
            throws Exception
    {
        assertFunction("hello_worlds('')", VARCHAR, "Hello World from scala");
    }

    @Test
    public void testHelloJohn()
            throws Exception
    {
        assertFunction("hello_worlds('John')", VARCHAR, "Hello John from scala");
    }
}
