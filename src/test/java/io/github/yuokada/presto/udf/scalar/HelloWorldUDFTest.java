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
        assertFunction("hello_world('John')", VARCHAR, "Hello John");
    }

    @Test(invocationCount = 100)
    public void testTranslatePerformance()
            throws Exception
    {
        assertFunction("translatej('foobarbaz', 'fb', 'FB')", VARCHAR, "FooBarBaz");
        assertFunction("translatej('translate', 'rnlt', '123')", VARCHAR, "1a2s3ae");
        assertFunction("translatej('translate', 'rnlt', '1234')", VARCHAR, "41a2s3a4e");
    }

}
