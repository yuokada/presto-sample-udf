package io.github.yuokada.presto.udf.scalar;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class KuromojiUDFTest
        extends AbstractTestFunctions
{
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

    @Test
    public void testKuromojiEmpty()
            throws Exception
    {
        assertFunction("kuromoji_tokenize('')",
                new ArrayType(VarcharType.createVarcharType(0)),
                ImmutableList.of());
    }
}
