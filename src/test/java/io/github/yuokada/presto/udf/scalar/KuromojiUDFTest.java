package io.github.yuokada.presto.udf.scalar;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

public class KuromojiUDFTest
        extends AbstractTestFunctions
{
    @DataProvider(name = "query-provider")
    public Object[][] dataProvider()
    {
        return new Object[][] {
                {"kuromoji_tokenize('5000兆円欲しい。')", 10,
                 ImmutableList.of("5000", "兆", "円", "欲しい", "。")},
                };
    }

    @DataProvider(name = "query-provider2")
    public Object[][] dataProvider2()
    {
        return new Object[][] {
                {"kuromoji_tokenize('5000兆円欲しい。', 'normal')", 10,
                 ImmutableList.of("5000", "兆", "円", "欲しい", "。")},
                {"kuromoji_tokenize('5000兆円欲しい。', 'search')", 10,
                 ImmutableList.of("5000", "兆", "円", "欲しい", "。")},
                {"kuromoji_tokenize('5000兆円欲しい。', 'extended')", 10,
                 ImmutableList.of("兆", "円", "欲しい", "。", "5", "0", "0", "0")},
                };
    }

    @BeforeClass
    public void setUp()
    {
        registerScalar(KuromojiUDF.class);
    }

    @Test(threadPoolSize = 5, invocationCount = 10)
    public void testKuromojiNormal()
            throws Exception
    {
        assertFunction("kuromoji_tokenize('5000兆円欲しい。')",
                new ArrayType(VarcharType.createVarcharType(10)),
                ImmutableList.of("5000", "兆", "円", "欲しい", "。"));
    }

    @Test(dataProvider = "query-provider")
    public void testKuromojiNormalDP(String query, Integer size, List<String> expect)
            throws Exception
    {
        assertFunction(query, new ArrayType(VarcharType.createVarcharType(size)), expect);
    }

    @Test(dataProvider = "query-provider2")
    public void testKuromojiNormalWithMode(String query, Integer size, List<String> expect)
            throws Exception
    {
        assertFunction(query, new ArrayType(VarcharType.createVarcharType(size)), expect);
    }

    @Test
    public void testKuromojiEmpty()
            throws Exception
    {
        assertFunction("kuromoji_tokenize('')",
                new ArrayType(VarcharType.createVarcharType(0)),
                ImmutableList.of());
    }

    @Test
    public void testInvalidArgument()
    {
        assertInvalidFunction("kuromoji_tokenize('', 'failValue')",
                "Invalid mode value: failValue");
    }
}
