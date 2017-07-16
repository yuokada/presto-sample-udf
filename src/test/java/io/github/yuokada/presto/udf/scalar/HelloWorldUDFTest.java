package io.github.yuokada.presto.udf.scalar;

import io.airlift.slice.Slices;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;

/**
 * Unit test for UDF.
 */
public class HelloWorldUDFTest
        extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public HelloWorldUDFTest(String testName)
    {
        super(testName);
    }

    @Test
    public void testHelloWorld()
            throws Exception
    {
        Assert.assertThat(HelloWorldUDF.helloworld(null).toStringUtf8(), is("Hello World"));

        Assert.assertThat(HelloWorldUDF.helloworld(Slices.utf8Slice("Joe")).
                toStringUtf8(), is("Hello Joe"));
    }
}
