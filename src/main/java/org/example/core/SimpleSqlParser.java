package org.example.core;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.example.core.dto.Ast;
import org.example.core.dto.InsertAst;
import org.example.core.dto.SelectAst;

public final class SimpleSqlParser {

    private static final Pattern INSERT =
            Pattern.compile("(?i)INSERT\\s+INTO\\s+(\\w+)\\s+VALUES\\s*\\(\\s*(\\d+)\\s*,\\s*'([^']*)'\\s*\\)\\s*;?");
    private static final Pattern SELECT = Pattern.compile("(?i)SELECT\\s+\\*\\s+FROM\\s+(\\w+)\\s*;?");

    public Ast parse(String sql) {
        String s = sql.trim();
        Matcher mi = INSERT.matcher(s);
        if (mi.matches()) {
            return new InsertAst(mi.group(1), Long.parseLong(mi.group(2)), mi.group(3));
        }
        Matcher ms = SELECT.matcher(s);
        if (ms.matches()) {
            return new SelectAst(ms.group(1));
        }
        throw new IllegalArgumentException("unsupported SQL: " + sql);
    }
}
