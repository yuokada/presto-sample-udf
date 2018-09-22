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
        assertFunction("hello_worlds('John')", VARCHAR, "Hello John from scala");
    }

    @Test
    public void testTranslate()
            throws Exception
    {
        assertFunction("translate('123def', '123',   'abc')", VARCHAR, "abcdef");
        assertFunction("translate('123def', '123',   'ab')", VARCHAR, "abdef");
        assertFunction("translate('123def', '12',    'abc')", VARCHAR, "ab3def");
        assertFunction("translate('123def', '1231',  'abcd')", VARCHAR, "abcdef");
        assertFunction("translate('hello',  'hello', 'hi')", VARCHAR, "hi");

        assertFunction("translate('foobarbaz', 'fb', 'FB')", VARCHAR, "FooBarBaz");
        assertFunction("translate('translate', 'rnlt', '123')", VARCHAR, "1a2s3ae");
        assertFunction("translate('translate', 'rnlt', '1234')", VARCHAR, "41a2s3a4e");
    }

    @Test(invocationCount = 100)
    public void testTranslatePerformance()
            throws Exception
    {
        assertFunction("translate('foobarbaz', 'fb', 'FB')", VARCHAR, "FooBarBaz");
        assertFunction("translate('translate', 'rnlt', '123')", VARCHAR, "1a2s3ae");
        assertFunction("translate('translate', 'rnlt', '1234')", VARCHAR, "41a2s3a4e");
    }

}
