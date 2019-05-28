package io.github.yuokada.presto.udf.scalar;

import com.atilika.kuromoji.TokenizerBase;
import com.atilika.kuromoji.ipadic.Token;
import com.atilika.kuromoji.ipadic.Tokenizer;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.LiteralParameters;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlNullable;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.Type;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class KuromojiUDF
{
    private static final Block zeroBlock = VARCHAR.createBlockBuilder(null, 0).build();

    // https://github.com/google/guava/wiki/CachesExplained
    private static final Cache<TokenizerKey, Tokenizer> cache = CacheBuilder.newBuilder()
            .maximumSize(32)
            .expireAfterAccess(600, TimeUnit.SECONDS)
            .build();
    // https://hivemall.incubator.apache.org/userguide/misc/tokenizer.html#japanese-tokenizer
    private static final List<String> modes = Arrays.asList("normal", "search", "extended");

    private static final List<String> partOfSpeeches = Arrays.asList("助詞", "助動詞", "助動詞", "記号");
    private static final Predicate<Token> partFilter = x -> !(partOfSpeeches.contains(x.getPartOfSpeechLevel1()));
    private static final Function<Token, String> getSurface = (Token t) -> {
        if (t.getBaseForm().equals("*")) {
            return t.getSurface().equals("/") ? "" : t.getSurface();
        }
        else {
            return t.getBaseForm();
        }
    };

    private KuromojiUDF()
    {
    }

    @Description("Now Working.")
    @ScalarFunction("kuromoji_tokenize")
    @LiteralParameters("x")
    @SqlType("array<varchar(x)>")
    public static Block kuromojiTokenize(@SqlNullable @SqlType("varchar(x)") Slice sentence)
    {
        String input = sentence.toStringUtf8();
        if (sentence == null || input.isEmpty()) {
            return zeroBlock;
        }

        return kuromojiTokenize(input, "normal", zeroBlock);
    }

    @ScalarFunction("kuromoji_tokenize")
    @LiteralParameters("x")
    @SqlType("array<varchar(x)>")
    public static Block kuromojiTokenize(@SqlNullable @SqlType("varchar(x)") Slice sentence, @SqlType("varchar(x)") Slice mode)
    {
        String modeString = mode.toStringUtf8();
        if (!modes.contains(modeString)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid mode value: " + modeString);
        }

        String input = sentence.toStringUtf8();
        if (sentence == null || input.isEmpty()) {
            return zeroBlock;
        }
        return kuromojiTokenize(input, modeString, zeroBlock);
    }

    @ScalarFunction("kuromoji_tokenize")
    @TypeParameter("T")
    @LiteralParameters("x")
    @SqlType("array<varchar(x)>")
    public static Block kuromojiTokenize(
            @TypeParameter("T") Type valueType,
            @SqlNullable @SqlType("varchar(x)") Slice sentence,
            @SqlType("varchar(x)") Slice mode,
            @SqlType("array(T)") Block arrayBlock)
    {
        // ref: https://github.com/prestodb/presto/blob/4128cb5fefe534fd42d267481838f3adabeb4e7b/presto-main/src/main/java/com/facebook/presto/operator/scalar/ArrayElementAtFunction.java#L92-L106
        String modeString = mode.toStringUtf8();
        if (!modes.contains(modeString)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid mode value: " + modeString);
        }

        String input = sentence.toStringUtf8();
        if (sentence == null || input.isEmpty()) {
            return zeroBlock;
        }
        return kuromojiTokenize(input, modeString, arrayBlock);
    }

    private static Block kuromojiTokenize(String input, String modeString, Block arrayBlock)
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, input.length() / 4);

        Tokenizer tokenizer = getTokenizerWithMandD(modeString, arrayBlock);
        tokenizer.tokenize(input).stream()
                .filter(partFilter)
                .map(getSurface)
                .filter(t -> !t.isEmpty())
                .forEach(
                        token -> {
                            VARCHAR.writeSlice(blockBuilder, utf8Slice(token));
                        });
        return blockBuilder.build();
    }

    /**
     * Modeと辞書からTokenizerを生成して返却。
     *
     * @param mode
     * @param userDictionary
     * @return
     */
    private static Tokenizer getTokenizerWithMandD(String mode, Block userDictionary)
    {
        StringBuffer dictionaryBuffer = new StringBuffer();
        int position = userDictionary.getPositionCount();
        List<String> words = new ArrayList<>();
        for (int i = 0; i < position; i++) {
            String entry = VARCHAR.getSlice(userDictionary, i).toStringUtf8();
            // NOTE: 単語をwordsに追加
            words.add(entry.substring(0, entry.indexOf(",")));
            dictionaryBuffer.append(entry + "\n");
        }

        TokenizerKey key = new TokenizerKey(mode, words.hashCode());
        Tokenizer tokenizer = cache.getIfPresent(key);

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

            try {
                InputStream is = new ByteArrayInputStream(
                        dictionaryBuffer.toString().getBytes());
                tokenizer = new Tokenizer.Builder()
                        .mode(modeObject)
                        .userDictionary(is)
                        .build();
                cache.put(key, tokenizer);
            }
            catch (IOException e) {
                // TODO: 不正な辞書を渡した時の処理を追加
                e.printStackTrace();
            }
        }
        return tokenizer;
    }

    private static class TokenizerKey
    {
        private static String mode;
        private static int dictHashCode;

        public TokenizerKey(String mode, int dictHashCode)
        {
            this.mode = mode;
            this.dictHashCode = dictHashCode;
        }

        @Override
        public int hashCode()
        {
            int result = 17;
            result += mode.hashCode();
            result = 31 * result + dictHashCode;
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (((TokenizerKey) obj).mode.equals(mode)
                    && ((TokenizerKey) obj).dictHashCode == dictHashCode) {
                return true;
            }
            else {
                return false;
            }
        }

        @Override
        public String toString()
        {
            return toStringHelper(this.getClass())
                    .add("mode", mode)
                    .add("dictHashCode", dictHashCode)
                    .toString();
        }
    }
}
