package org.example.core.exec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Map;
import org.example.core.catalog.Catalog;
import org.example.core.dto.ExecutionContext;
import org.example.core.dto.ExecutionResult;
import org.example.core.dto.Row;
import org.example.core.dto.TableDescriptor;
import org.example.core.dto.TableId;
import org.example.core.schema.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SqlPipelineTest {

    @Test
    void parseAnalyzeExecuteInsertSelect(@TempDir Path tmp) throws Exception {
        Path heap = tmp.resolve("sql.heap");
        Path pk = tmp.resolve("sql.pk");
        Schema schema = Schema.defaultRowSchema();

        Catalog catalog = new Catalog();
        catalog.register(new TableDescriptor(new TableId("items"), schema, heap, pk, Map.of()));

        var sql = new SqlEngine();
        ExecutionContext ctx = new ExecutionContext(catalog);

        assertTrue(sql.execute("INSERT INTO items VALUES (10, 'hello');", ctx).message().contains("inserted"));

        ExecutionResult res = sql.execute("SELECT * FROM items", ctx);
        assertEquals(1, res.rows().size());
        assertEquals(new Row(10L, "hello"), res.rows().get(0));
    }
}
