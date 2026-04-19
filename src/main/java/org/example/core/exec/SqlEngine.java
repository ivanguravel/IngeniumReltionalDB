package org.example.core.exec;

import java.io.IOException;
import org.example.core.PassThroughOptimizer;
import org.example.core.SimpleAnalyzer;
import org.example.core.SimpleSqlParser;
import org.example.core.dto.ExecutionContext;
import org.example.core.dto.ExecutionResult;

/** Parses minimal SQL, analyzes, optimizes, and runs against {@link ExecutionContext#catalog()}. */
public final class SqlEngine {

    private final SimpleSqlParser parser = new SimpleSqlParser();
    private final SimpleAnalyzer analyzer = new SimpleAnalyzer();
    private final PassThroughOptimizer optimizer = new PassThroughOptimizer();
    private final DefaultExecutor executor = new DefaultExecutor();

    public ExecutionResult execute(String sql, ExecutionContext ctx) throws IOException {
        var ast = parser.parse(sql);
        var plan = optimizer.optimize(analyzer.analyze(ast));
        return executor.execute(plan, ctx);
    }
}
