package io.github.yuokada.presto.udf.scalar;

import com.atilika.kuromoji.TokenizerBase;
import com.atilika.kuromoji.ipadic.Tokenizer;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.airlift.slice.Slice;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;

public class KuromojiUDF
{
    private static final Tokenizer tokenizer = new Tokenizer();
    private static final Block zeroBlock = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 0).build();

    private static Cache<String, Tokenizer> modeCache = CacheBuilder.newBuilder()
            .maximumSize(3)
            .build();

    // https://github.com/google/guava/wiki/CachesExplained
    private static final Cache<String, Tokenizer> cache = CacheBuilder.newBuilder()
            .maximumSize(256)
            .build();
    // https://hivemall.incubator.apache.org/userguide/misc/tokenizer.html#japanese-tokenizer
    private static final List<String> modes = Arrays.asList("normal", "search", "extended");

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

    @Description("Now Working.")
    @ScalarFunction("kuromoji_tokenize")
    @LiteralParameters("x")
    @SqlType("array<varchar(x)>")
    public static Block kuromojii(@SqlNullable @SqlType("varchar(x)") Slice sentence, @SqlType("varchar(x)") Slice mode)
    {
        String modeString = mode.toStringUtf8();
        if (!modes.contains(modeString)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid mode value: " + modeString);
        }

        if (sentence == null || sentence.toStringUtf8().isEmpty()) {
            return zeroBlock;
        }

        String input = sentence.toStringUtf8();
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), input.length() / 4);

        Tokenizer tokenizer = getTokenizerWithMode(modeString);
        tokenizer.tokenize(input).forEach(
                token -> {
                    VARCHAR.writeSlice(blockBuilder, utf8Slice(token.getSurface()));
                });
        return blockBuilder.build();
    }

    private static Tokenizer getTokenizerWithMode(String mode)
    {
        Tokenizer tokenizer = modeCache.getIfPresent(mode);
        if (tokenizer == null) {
            TokenizerBase.Mode modeObject;
            switch (mode) {
                case "search":
                    modeObject = Tokenizer.Mode.SEARCH;
                    break;
                case "extended":
                    modeObject = Tokenizer.Mode.EXTENDED;
                    break;
                case "normal":
                default:
                    modeObject = Tokenizer.Mode.NORMAL;
            }
            tokenizer = new Tokenizer.Builder().mode(modeObject).build();
            modeCache.put(mode, tokenizer);
        }
        return tokenizer;
    }
}
