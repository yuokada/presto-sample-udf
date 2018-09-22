package io.github.yuokada.presto.udf.scalar;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static io.airlift.slice.Slices.utf8Slice;

public class HelloWorldUDF
{
    private HelloWorldUDF()
    {
    }

    @Description("Hello World(UDF Practice)")
    @ScalarFunction("hello_world")
    @SqlType(VARCHAR)
    public static Slice helloworld(@SqlNullable @SqlType(VARCHAR) Slice name)
    {
        if (name == null || name.toStringUtf8().isEmpty()) {
            return utf8Slice("Hello World");
        }
        else {
            return utf8Slice(String.format("Hello %s", name.toStringUtf8()));
        }
    }

    // see: https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF
    @Description("the input string by replacing the characters present in the from string with the corresponding characters in the to string")
    @ScalarFunction("translatej")
    @SqlType(VARCHAR)
    public static Slice translate(
            @SqlNullable @SqlType(VARCHAR) Slice string,
            @SqlType(VARCHAR) Slice from,
            @SqlType(VARCHAR) Slice to)
    {
        if (string == null) {
            return null;
        }
        Map<Integer, Integer> replacementMap = new HashMap<Integer, Integer>();
        Set<Integer> deletionSet = new HashSet<Integer>();
        replacementMap.clear();
        deletionSet.clear();
        String fromAscii = from.toStringAscii();
        String toAscii = to.toStringAscii();
        ByteBuffer fromBytes = ByteBuffer.wrap(fromAscii.getBytes());
        ByteBuffer toBytes = ByteBuffer.wrap(toAscii.getBytes());
        while (fromBytes.hasRemaining()) {
            int fromCodePoint = fromBytes.get();
            if (toBytes.hasRemaining()) {
                int toCodePoint = toBytes.get();
                if (replacementMap.containsKey(fromCodePoint) || deletionSet.contains(fromCodePoint)) {
                    continue;
                }
                replacementMap.put(fromCodePoint, toCodePoint);
            }
            else {
                if (replacementMap.containsKey(fromCodePoint) || deletionSet.contains(fromCodePoint)) {
                    continue;
                }
                deletionSet.add(fromCodePoint);
            }
        }
        String input = string.toStringAscii();
        StringBuilder sb = new StringBuilder();
        ByteBuffer inputBytes = ByteBuffer.wrap(input.getBytes(), 0, input.length());
        while (inputBytes.hasRemaining()) {
            int inputCodePoint = inputBytes.get();
            if (deletionSet.contains(inputCodePoint)) {
                continue;
            }
            Integer replacementCodePoint = replacementMap.get(inputCodePoint);
            char[] charArray = Character.toChars((replacementCodePoint != null) ? replacementCodePoint : inputCodePoint);
            sb.append(charArray);
        }
        String answer = sb.toString();
        return Slices.utf8Slice(answer);
    }
}
