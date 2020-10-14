package com.nosqldriver.jdbc.security;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PatternTransformer implements DataTransformer<String> {
    private final Pattern pattern;
    private final String replacement;

    public PatternTransformer(Pattern pattern, String replacement) {
        this.pattern = pattern;
        this.replacement = replacement;
    }

    @Override
    public String apply(String str) {
        Matcher m = pattern.matcher(str);
        return m.replaceAll(replacement);
    }

    @Override
    public Integer get() {
        return 1;
    }
}
