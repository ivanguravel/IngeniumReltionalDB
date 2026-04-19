package org.example.core.exec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.example.core.dto.AnalyzedStatement;
import org.example.core.dto.ExecutionContext;
import org.example.core.dto.ExecutionResult;
import org.example.core.dto.Row;
import org.example.core.storage.TableStorage;

public final class DefaultExecutor {

    public ExecutionResult execute(AnalyzedStatement plan, ExecutionContext ctx) throws IOException {
        try (TableStorage table = ctx.catalog().openTable(plan.tableName())) {
            return switch (plan.kind()) {
                case INSERT -> {
                    Row row = plan.row();
                    if (row == null) {
                        throw new IllegalStateException("INSERT without row");
                    }
                    table.insert(row);
                    yield ExecutionResult.message("inserted");
                }
                case SELECT -> {
                    List<Row> out = new ArrayList<>();
                    table.scan().forEachRemaining(out::add);
                    yield ExecutionResult.rows(out);
                }
            };
        }
    }
}
