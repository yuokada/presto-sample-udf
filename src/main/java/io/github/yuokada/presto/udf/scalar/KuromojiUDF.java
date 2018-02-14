package io.github.yuokada.presto.udf.scalar;

import com.atilika.kuromoji.ipadic.Token;
import com.atilika.kuromoji.ipadic.Tokenizer;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import java.util.List;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;

public class KuromojiUDF
{

    private final static Tokenizer tokenizer = new Tokenizer();

    @Description("Now Working.")
    @ScalarFunction("kuromoji_tokenize")
    @LiteralParameters("x")
    @SqlType("array<varchar(x)>")
    public static Block kuromojii(@SqlNullable @SqlType("varchar(x)") Slice sentence)
    {
        if (sentence == null || sentence.toStringUtf8().isEmpty()) {
            return VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 0).build();
        }

        String input = sentence.toStringUtf8();
        List<Token> tokens = tokenizer.tokenize(input);

        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 32);
        for (Token token : tokens) {
            VARCHAR.writeSlice(blockBuilder, utf8Slice(token.getSurface()));
        }
        return blockBuilder.build();
    }
}
