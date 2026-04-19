package org.example.core;

import org.example.core.dto.AnalyzedStatement;

public final class PassThroughOptimizer {

    public AnalyzedStatement optimize(AnalyzedStatement statement) {
        return statement;
    }
}
