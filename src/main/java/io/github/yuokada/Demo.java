package io.github.yuokada;

import com.atilika.kuromoji.ipadic.Token;
import com.atilika.kuromoji.ipadic.Tokenizer;
import com.atilika.kuromoji.ipadic.Tokenizer.Builder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

public class Demo
{

    public static void main(String[] args)
    {
        // https://qiita.com/masato_ka/items/2bae7f1e8ce245443947
//        String input = "井口裕香ちゃんと一風堂のラーメンが食べたい";
        String input = "kuromojiを使った分かち書きのテストです。第二引数にはnormal/search/extendedを指定できます。デフォルトではnormalモードです。";
        String userDict = "# 単語,　形態素解析結果の単語, 読み, 品詞\n" +
                "井口裕香,井口裕香,イグチユカ,名詞\n" +
                "一風堂,一風堂,イップウドウ,名詞\n" +
                "一蘭,一蘭,イチラン,名詞\n" +
                "元祖長浜,元祖長浜,ガンソナガハマ,名詞\n";
        InputStream inputStream = new ByteArrayInputStream(userDict.getBytes());
        Builder builder = new Tokenizer.Builder();
        Tokenizer tokenizer = null;
        try {
            tokenizer = builder.mode(Tokenizer.Mode.NORMAL)
                    .userDictionary(inputStream)
                    .build();
        }
        catch (IOException e1) {

        }
        List<Token> tokens = tokenizer.tokenize(input);

        List<String> PartOfSpeeches = Arrays.asList("助詞", "助動詞", "助動詞", "記号");
        Predicate<Token> partFilter = x -> !(PartOfSpeeches.contains(x.getPartOfSpeechLevel1()));
        Function<Token, String> getSurface = (Token t) -> {
            if (t.getBaseForm().equals("*")) {
                return t.getSurface().equals("/") ? "" : t.getSurface();
            }
            else {
                return t.getBaseForm();
            }
        };

        tokens
                .stream()
                .filter(partFilter)
                .map(getSurface)
                .filter(t -> !t.isEmpty())
                .forEach(System.out::println);
    }
}
