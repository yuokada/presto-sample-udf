package io.github.yuokada.presto.udf.scalar;

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

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;

public class KuromojiUDF
{
    private static final Tokenizer tokenizer = new Tokenizer();
    private static final Block zeroBlock = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 0).build();

    @Description("Now Working.")
    @ScalarFunction("kuromoji_tokenize")
    @LiteralParameters("x")
    @SqlType("array<varchar(x)>")
    public static Block kuromojii(@SqlNullable @SqlType("varchar(x)") Slice sentence)
    {
        if (sentence == null || sentence.toStringUtf8().isEmpty()) {
            return zeroBlock;
        }

        String input = sentence.toStringUtf8();
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), input.length() / 4);

        tokenizer.tokenize(input).forEach(
                token -> {
                    VARCHAR.writeSlice(blockBuilder, utf8Slice(token.getSurface()));
//            System.out.println(token.getPartOfSpeechLevel1().toString()); // => 名詞, 形容詞 or etc
                });
        return blockBuilder.build();
    }
}
