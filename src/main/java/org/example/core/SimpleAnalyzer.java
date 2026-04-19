package org.example.core;

import org.example.core.dto.Ast;
import org.example.core.dto.InsertAst;
import org.example.core.dto.SelectAst;
import org.example.core.dto.AnalyzedStatement;
import org.example.core.dto.Row;

public final class SimpleAnalyzer {

    public AnalyzedStatement analyze(Ast ast) {
        return switch (ast) {
            case InsertAst i -> new AnalyzedStatement(
                    i.tableName(), AnalyzedStatement.Kind.INSERT, new Row(i.id(), i.text()));
            case SelectAst s -> new AnalyzedStatement(s.tableName(), AnalyzedStatement.Kind.SELECT, null);
        };
    }
}
